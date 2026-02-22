import requests
import json
import os
import time
import threading
import random
import sqlite3
import websocket
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, jsonify, request

app = Flask(__name__)

# ---- IST Timezone (UTC+5:30) ----
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Current datetime in IST."""
    return datetime.now(IST)

def ts_to_ist(timestamp):
    """Convert a Unix timestamp to an IST-aware datetime."""
    return datetime.fromtimestamp(timestamp, tz=IST)

API_URL = "https://reports.yourbus.in/ci/trackApp"
BUS_FILE = "buses.json"
DB_FILE = "/data/fleet.db"
VALHALLA_URL = "https://valhalla1.openstreetmap.de"
CHECK_INTERVAL = 10

STOP_THRESHOLD = 120          # 2 minutes
JOURNEY_END_THRESHOLD = 3600  # 1 hour
STABLE_RADIUS_KM = 0.5        # 500 meters
MOVEMENT_THRESHOLD = 5        # speed > 5 km/h means moving
IDLE_THRESHOLD = 120          # 2 minutes idle
RESUME_DISTANCE_KM = 0.3      # must move at least 300m
END_CONFIRM_THRESHOLD = 3600  # 1 hour idle → end journey
BUSES_FILE = "buses.json"
ws_started = {}

print("DB exists:", os.path.exists("/data/fleet.db"))

def load_buses():
    try:
        with open(BUSES_FILE, "r") as f:
            buses = json.load(f)
            return buses
    except Exception as e:
        print("Error loading buses.json:", e)
        return []


fleet_state = {}
service_vehicle_map = {}


# ================= DATABASE =================

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS journeys (
            journey_id TEXT PRIMARY KEY,
            bus_no TEXT,
            departure_date TEXT,
            start_timestamp INTEGER,
            end_timestamp INTEGER,
            status TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS trip_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            journey_id TEXT,
            timestamp INTEGER,
            lat REAL,
            lon REAL,
            speed REAL
        )
    """)

    conn.commit()
    conn.close()

# ================= UTIL =================

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

def generate_journey_id(bus_no):
    return f"{bus_no}_{int(time.time())}"

def get_active_journey(bus_no):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT journey_id
        FROM journeys
        WHERE bus_no = ?
        AND status = 'active'
        ORDER BY start_timestamp DESC
        LIMIT 1
    """, (bus_no,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def create_new_journey(bus_no, timestamp):
    journey_id = generate_journey_id(bus_no)
    departure_date = ts_to_ist(timestamp).strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT INTO journeys (journey_id, bus_no, departure_date, start_timestamp, status)
        VALUES (?, ?, ?, ?, 'active')
    """, (journey_id, bus_no, departure_date, timestamp))
    conn.commit()
    conn.close()
    return journey_id

def end_journey(journey_id, timestamp):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        UPDATE journeys
        SET status = 'ended', end_timestamp = ?
        WHERE journey_id = ?
    """, (timestamp, journey_id))
    conn.commit()
    conn.close()

_last_stale_check = 0   # module-level: tracks when we last ran cleanup

def auto_end_stale_journeys():
    """
    Runs automatically inside the tracking loop every 10 minutes.
    Ends any active journey whose last GPS ping is more than 1 hour old.
    This handles:
      - Buses that stopped pinging after reaching destination
      - Buses whose GPS device went offline/sleep
      - Journeys that survived a server restart mid-idle
    End timestamp is set to the last actual ping time, not current time.
    """
    global _last_stale_check
    now = int(time.time())

    # Only run once every 10 minutes
    if now - _last_stale_check < 600:
        return

    _last_stale_check = now
    cutoff = now - 3600   # pings older than 1 hour

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT j.journey_id,
               j.bus_no,
               j.departure_date,
               MAX(tp.timestamp) as last_ping
        FROM journeys j
        LEFT JOIN trip_points tp ON tp.journey_id = j.journey_id
        WHERE j.status = 'active'
        GROUP BY j.journey_id
        HAVING last_ping < ? OR last_ping IS NULL
    """, (cutoff,))
    stale = c.fetchall()
    conn.close()

    if not stale:
        return

    print(f"[STALE CLEANUP] Found {len(stale)} stale journey(s)")
    for journey_id, bus_no, dep_date, last_ping in stale:
        end_ts = last_ping if last_ping else now
        idle_min = round((now - end_ts) / 60)
        end_journey(journey_id, end_ts)
        # Also clear in-memory state so next departure creates a fresh journey
        if bus_no in fleet_state:
            fleet_state[bus_no]["idle_start_time"] = None
            fleet_state[bus_no]["idle_start_location"] = None
        print(f"[STALE END] {bus_no} / {dep_date} — idle {idle_min} min")


# ================= TRACKING LOOP =================

def tracking_loop():

    print("Tracking loop started")

    while True:

        try:

            buses = load_buses()

            for bus in buses:
                tracking_type = bus.get("tracking_type", "trackapp")

                if tracking_type == "websocket":
                    bus_no = bus.get("serviceNo")
                    if not bus_no:
                        continue
                else:
                    bus_no = bus["bus_no"]

                tracking_type = bus.get("tracking_type", "trackapp")

                # =====================
                # WEBSOCKET BUSES
                # =====================

                if tracking_type == "websocket":
                    service_no = bus.get("serviceNo")

                    if service_no not in ws_started:
                        ws_started[service_no] = True
                        threading.Thread(
                            target=websocket_listener,
                            args=(bus,),
                            daemon=True
                        ).start()

                    continue


                # =====================
                # TRACKAPP BUSES
                # =====================

                device_id = bus.get("device_id")
                auth = bus.get("auth")

                if not device_id or not auth:
                    continue

                url = "https://reports.yourbus.in/ci/trackApp"
                headers = {
                    "Content-Type": "application/json",
                    "Authentication": auth
                }
                payload = {
                    "o": bus.get("operator", ""),
                    "v": bus_no,
                    "g": device_id
                }

                response = requests.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=10
                )

                data = response.json()

                if data["msg"] != "Ok":
                    continue

                gps = data["data"]

                lat = float(gps["lt"])
                lon = float(gps["lg"])
                speed = float(gps["sp"])
                timestamp = int(time.time())

                if bus_no not in fleet_state:
                    fleet_state[bus_no] = {
                        "state": "ACTIVE",
                        "idle_start_time": None,
                        "idle_start_location": None,
                        "last_timestamp": timestamp,
                        "last_location": None
                    }

                active_journey = get_active_journey(bus_no)

                # NOTE: Date-change logic removed intentionally.
                # Journeys for overnight runs must NOT be split at midnight.
                # Journey end is determined solely by 1-hour idle threshold below.

                state = fleet_state[bus_no]

                # ================= IDLE DETECTION =================
                # ================= IDLE DETECTION =================
                # Movement is measured from the idle START POINT, not from the
                # previous poll. This means GPS drift between consecutive pings
                # never resets the timer — only genuine movement away from the
                # parked location does.

                last_loc = state.get("last_location")

                if last_loc:
                    movement_from_last = haversine(last_loc[0], last_loc[1], lat, lon)
                else:
                    movement_from_last = 0

                # Determine movement from idle anchor point (where bus first stopped)
                if state["idle_start_location"]:
                    movement_from_anchor = haversine(
                        state["idle_start_location"][0],
                        state["idle_start_location"][1],
                        lat, lon
                    )
                else:
                    movement_from_anchor = movement_from_last

                # Bus is considered idle if it hasn't moved > 300m from its anchor.
                # Using anchor (not last_loc) means GPS drift between polls
                # never accidentally resets a genuine parked idle timer.
                IDLE_ANCHOR_RADIUS = 0.3   # 300 metres from where bus first stopped

                if movement_from_anchor <= IDLE_ANCHOR_RADIUS:

                    if state["idle_start_time"] is None:
                        # First poll where bus appears stopped — stamp the anchor
                        state["idle_start_time"] = timestamp
                        state["idle_start_location"] = (lat, lon)

                    idle_duration = timestamp - state["idle_start_time"]

                    if active_journey and idle_duration >= 3600:
                        print(f"[API JOURNEY END] {bus_no} after {idle_duration//60} min idle")
                        end_journey(active_journey, timestamp)
                        active_journey = None          # ← do NOT create new journey here
                        state["idle_start_time"] = None
                        state["idle_start_location"] = None

                else:
                    # Bus has genuinely moved away from anchor — reset timer
                    state["idle_start_time"] = None
                    state["idle_start_location"] = None

                # ================= CREATE NEW JOURNEY =================

                if active_journey is None:
                    # Only create a journey if the bus is actually moving
                    if speed > MOVEMENT_THRESHOLD:
                        active_journey = create_new_journey(bus_no, timestamp)
                        print(f"[NEW JOURNEY AFTER REAL MOVEMENT] {bus_no}")
                    else:
                        print(f"[SKIP] {bus_no} stationary at restart, no journey created")
                        continue

                state["last_location"] = (lat, lon)
                state["last_timestamp"] = timestamp

                conn = sqlite3.connect(DB_FILE)
                c = conn.cursor()
                c.execute("""
                    INSERT INTO trip_points
                    (journey_id, timestamp, lat, lon, speed)
                    VALUES (?, ?, ?, ?, ?)
                """, (active_journey, timestamp, lat, lon, speed))
                conn.commit()
                conn.close()

                print(f"[API] {bus_no} → {lat},{lon}")

        except Exception as e:
            print("Tracking Error:", e)

        auto_end_stale_journeys()   # ← runs every 10 min, skips otherwise
        time.sleep(10)


def websocket_listener(bus):
    service_no = bus.get("serviceNo")
    bus_no = service_no

    if not service_no:
        print(f"[WS] {bus_no} skipped, no serviceNo")
        return

    def on_open(ws):
        payload = {
            "serviceNo": service_no,
            "doj": now_ist().strftime("%Y-%m-%d"),
            "trackingType": "full-tracking"
        }
        ws.send(json.dumps(payload))
        print(f"[WS] Connected → {bus_no}")

    def on_message(ws, message):
        try:
            data = json.loads(message)

            if not data.get("success"):
                return

            position = data.get("vehicleInfo", {}).get("position", {})
            vehicle_number = data.get("vehicleInfo", {}).get("registrationNumber")

            if vehicle_number:
                service_vehicle_map[service_no] = vehicle_number
                print(f"[WS VEHICLE] {service_no} using vehicle {vehicle_number}")

            if not position:
                return

            lat = float(position["latitude"])
            lon = float(position["longitude"])
            speed = 0
            timestamp = int(time.time())

            bus_no = service_no

            # ================= INIT STATE =================

            if bus_no not in fleet_state:
                fleet_state[bus_no] = {
                    "idle_start_time": None,
                    "idle_location": None,
                    "last_location": None,
                    "last_signal_time": timestamp
                }

            state = fleet_state[bus_no]

            # ================= GET ACTIVE JOURNEY FIRST =================
            active_journey = get_active_journey(bus_no)

            # ================= GPS LOSS DETECTION =================
            if state.get("last_signal_time"):
                signal_gap = timestamp - state["last_signal_time"]
                if signal_gap >= 3600:
                    print("[GPS LOSS END JOURNEY]", bus_no)
                    if active_journey:
                        end_journey(active_journey, timestamp)
                        active_journey = create_new_journey(bus_no, timestamp)

            state["last_signal_time"] = timestamp

            # Re-fetch after possible GPS loss journey change
            active_journey = get_active_journey(bus_no)

            # NOTE: Date-change logic removed intentionally.
            # Overnight journeys must NOT be split at midnight.
            # Journey end is controlled solely by 1-hour idle / GPS loss criteria.

            # ================= NO ACTIVE JOURNEY — WAIT FOR MOVEMENT =================
            if active_journey is None:
                last_loc = state.get("last_location")
                moved = False
                if last_loc:
                    moved = haversine(last_loc[0], last_loc[1], lat, lon) > RESUME_DISTANCE_KM
                if moved:
                    active_journey = create_new_journey(bus_no, timestamp)
                    print(f"[FIRST JOURNEY][WS] {bus_no}")
                else:
                    state["last_location"] = (lat, lon)
                    return  # wait for actual movement

            # ================= IDLE DETECTION =================
            if active_journey:
                last_location = state.get("last_location")

                if last_location:
                    distance = haversine(last_location[0], last_location[1], lat, lon)

                    if distance <= 0.05:
                        if state["idle_start_time"] is None:
                            state["idle_start_time"] = timestamp
                            state["idle_location"] = (lat, lon)
                    else:
                        movement = haversine(
                            state["last_location"][0],
                            state["last_location"][1],
                            lat, lon
                        )

                        if movement <= 0.15:
                            if state["idle_start_time"] is None:
                                state["idle_start_time"] = timestamp
                                state["idle_location"] = (lat, lon)
                            else:
                                idle_duration = timestamp - state["idle_start_time"]
                                idle_distance = haversine(
                                    state["idle_location"][0],
                                    state["idle_location"][1],
                                    lat, lon
                                )
                                if active_journey and idle_duration >= 3600 and idle_distance <= 0.3:
                                    print("[WS JOURNEY END]", bus_no)
                                    end_journey(active_journey, timestamp)
                                    active_journey = None   # ✅ wait for real movement
                                    state["idle_start_time"] = None
                                    state["idle_location"] = None
                        else:
                            state["idle_start_time"] = None
                            state["idle_location"] = None

            state["last_location"] = (lat, lon)

            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute("""
                INSERT INTO trip_points
                (journey_id, timestamp, lat, lon, speed)
                VALUES (?, ?, ?, ?, ?)
            """, (active_journey, timestamp, lat, lon, speed))
            conn.commit()
            conn.close()

            print(f"[WS] {bus_no} → {lat},{lon}")

        except Exception as e:
            print("[WS ERROR]", e)

    def on_close(ws, close_status_code, close_msg):
        print(f"[WS CLOSED] {bus_no}")
        while True:
            try:
                time.sleep(5)
                websocket_listener(bus)
                break
            except:
                continue

    ws = websocket.WebSocketApp(
        "wss://reports.yourbus.in:1029",
        on_open=on_open,
        on_message=on_message,
        on_close=on_close
    )
    ws.run_forever()


# ================= ROUTES =================

@app.route("/")
def home():
    return render_template("map.html")

@app.route("/buses")
def get_buses():
    with open(BUS_FILE) as f:
        buses = json.load(f)
    result = []
    for b in buses:
        if b.get("tracking_type") == "websocket":
            service = b.get("serviceNo")
            vehicle = service_vehicle_map.get(service)
            result.append({
                "id": service,
                "label": vehicle if vehicle else service
            })
        else:
            bus_no = b.get("bus_no")
            result.append({
                "id": bus_no,
                "label": bus_no
            })
    return jsonify(result)


@app.route("/dates/<bus_no>")
def get_dates(bus_no):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT DISTINCT departure_date
        FROM journeys
        WHERE bus_no = ?
        ORDER BY departure_date DESC
    """, (bus_no,))
    rows = c.fetchall()
    conn.close()
    return jsonify([r[0] for r in rows])

@app.route("/all-dates")
def get_all_dates():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT DISTINCT departure_date
        FROM journeys
        ORDER BY departure_date DESC
    """)
    rows = c.fetchall()
    conn.close()
    return jsonify([r[0] for r in rows])


@app.route("/route/<bus_no>/<path:departure_date>")
def get_route(bus_no, departure_date):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT journey_id, status
        FROM journeys
        WHERE bus_no = ?
        AND departure_date = ?
        ORDER BY start_timestamp
    """, (bus_no, departure_date))
    journeys = c.fetchall()
    all_points = []

    if journeys:
        last_status = journeys[-1][1]
        ended = (last_status == "ended")
    else:
        ended = False

    for jid, status in journeys:
        c.execute("""
            SELECT lat, lon, timestamp, speed
            FROM trip_points
            WHERE journey_id = ?
            ORDER BY timestamp
        """, (jid,))
        all_points.extend(c.fetchall())

    conn.close()
    return jsonify({
        "points": all_points,
        "ended": ended
    })


@app.route("/measure", methods=["POST"])
def measure():
    data = request.json
    bus_no = data["bus_no"]
    departure_date = data["trip_date"]
    start_ts = data["start_ts"]
    end_ts = data["end_ts"]

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT lat, lon, timestamp
        FROM trip_points tp
        JOIN journeys j ON tp.journey_id = j.journey_id
        WHERE j.bus_no = ?
        AND j.departure_date = ?
        AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp
    """, (bus_no, departure_date, start_ts, end_ts))
    rows = c.fetchall()
    conn.close()

    time_diff = end_ts - start_ts
    hours = time_diff // 3600
    minutes = (time_diff % 3600) // 60

    # ── Road-matched distance via Valhalla ──
    # match_points_valhalla returns [lat, lon] coords snapped to actual roads.
    # Summing those gives true road distance, same as what the map draws.
    matched = match_points_valhalla(rows)

    if matched and len(matched) >= 2:
        distance = 0
        for i in range(1, len(matched)):
            distance += haversine(matched[i-1][0], matched[i-1][1],
                                  matched[i][0],   matched[i][1])
    else:
        # Fallback to raw GPS sum if Valhalla fails
        distance = 0
        for i in range(1, len(rows)):
            distance += haversine(rows[i-1][0], rows[i-1][1],
                                  rows[i][0],   rows[i][1])

    return jsonify({
        "distance_km": round(distance, 2),
        "hours": hours,
        "minutes": minutes
    })

# ================= OSRM MAP MATCHING =================
def match_points_valhalla(rows):
    """
    Uses Valhalla /trace_route for true GPS map matching.
    Returns FULL road geometry (every curve and bend),
    not just the snapped GPS points.
    """
    if len(rows) < 2:
        print("[VALHALLA] Not enough points:", len(rows))
        return []

    # Step 1 — Filter GPS jitter (skip points < 20 metres apart)
    filtered = [rows[0]]
    for row in rows[1:]:
        last = filtered[-1]
        if haversine(last[0], last[1], row[0], row[1]) > 0.02:
            filtered.append(row)
    rows = filtered
    print(f"[VALHALLA] After jitter filter: {len(rows)} points")

    if len(rows) < 2:
        return []

    # Step 2 — Split on gaps > 10 minutes
    MAX_GAP_SECONDS = 600
    segments = []
    current = [rows[0]]

    for row in rows[1:]:
        gap = row[2] - current[-1][2]
        if gap > MAX_GAP_SECONDS:
            if len(current) >= 2:
                segments.append(current)
            current = [row]
        else:
            current.append(row)

    if len(current) >= 2:
        segments.append(current)

    print(f"[VALHALLA] Split into {len(segments)} segments")

    matched_coords = []
    BATCH = 100

    for seg_idx, segment in enumerate(segments):
        seg_start = ts_to_ist(segment[0][2]).strftime("%H:%M")
        seg_end   = ts_to_ist(segment[-1][2]).strftime("%H:%M")
        print(f"[VALHALLA] Segment {seg_idx}: {len(segment)} pts, {seg_start} to {seg_end}")

        for batch_start in range(0, len(segment), BATCH - 1):
            batch = segment[batch_start : batch_start + BATCH]
            if len(batch) < 2:
                continue

            # Build shape for Valhalla
            shape = [
                {"lat": round(lat, 6), "lon": round(lon, 6)}
                for lat, lon, ts in batch
            ]

            payload = {
                "shape": shape,
                "costing": "auto",
                "shape_match": "map_snap",
                # encoded_polyline6 gives full road geometry with all curves
                "trace_options": {
                    "search_radius": 50,
                    "interpolation_distance": 5
                }
            }

            try:
                time.sleep(0.3)

                res = requests.post(
                    "https://valhalla1.openstreetmap.de/trace_route",
                    json=payload,
                    timeout=30
                )
                print(f"[VALHALLA] Seg{seg_idx} batch{batch_start}: HTTP {res.status_code}")

                if res.status_code != 200:
                    print(f"[VALHALLA] Error: {res.text[:300]}")
                    matched_coords.extend([[lat, lon] for lat, lon, ts in batch])
                    continue

                data = res.json()

                # Extract full road geometry from each leg of each trip
                trips = data.get("trip", {})
                legs  = trips.get("legs", [])
                print(f"[VALHALLA] Legs: {len(legs)}")

                leg_coords = []
                for leg in legs:
                    # Decode the encoded polyline6 to get every road coordinate
                    encoded = leg.get("shape", "")
                    if encoded:
                        decoded = decode_polyline6(encoded)
                        leg_coords.extend(decoded)
                        print(f"[VALHALLA] Leg shape decoded: {len(decoded)} coords")

                if leg_coords:
                    matched_coords.extend(leg_coords)
                else:
                    print(f"[VALHALLA] No leg geometry — using raw points")
                    matched_coords.extend([[lat, lon] for lat, lon, ts in batch])

            except Exception as e:
                print(f"[VALHALLA] Exception seg{seg_idx} batch{batch_start}: {e}")
                matched_coords.extend([[lat, lon] for lat, lon, ts in batch])

    print(f"[VALHALLA] Total matched coords: {len(matched_coords)}")
    return matched_coords


def decode_polyline6(encoded):
    """
    Decodes a Valhalla encoded polyline6 string into [lat, lon] pairs.
    Valhalla uses precision=6 (multiply by 1e6), unlike Google's precision=5.
    """
    coords = []
    index = 0
    lat = 0
    lon = 0

    while index < len(encoded):
        # Decode latitude
        result = 0
        shift = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1f) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat

        # Decode longitude
        result = 0
        shift = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1f) << shift
            shift += 5
            if b < 0x20:
                break
        dlon = ~(result >> 1) if (result & 1) else (result >> 1)
        lon += dlon

        # Valhalla precision is 1e6
        coords.append([lat / 1e6, lon / 1e6])

    return coords


@app.route("/route-matched/<bus_no>/<path:departure_date>")
def route_matched(bus_no, departure_date):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT lat, lon, timestamp
        FROM trip_points tp
        JOIN journeys j
        ON tp.journey_id = j.journey_id
        WHERE bus_no = ?
        AND departure_date LIKE ?
        ORDER BY timestamp
    """, (bus_no, departure_date + "%"))
    rows = c.fetchall()
    conn.close()

    print(f"[ROUTE-MATCHED] {bus_no} {departure_date} - {len(rows)} raw points from DB")

    matched = match_points_valhalla(rows)

    print(f"[ROUTE-MATCHED] Returning {len(matched)} matched coords")
    return jsonify(matched)

@app.route("/debug-gaps/<bus_no>/<path:departure_date>")
def debug_gaps(bus_no, departure_date):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT lat, lon, timestamp, speed
        FROM trip_points tp
        JOIN journeys j ON tp.journey_id = j.journey_id
        WHERE bus_no = ?
        AND departure_date LIKE ?
        ORDER BY timestamp
    """, (bus_no, departure_date + "%"))
    rows = c.fetchall()
    conn.close()

    result = []
    for i, row in enumerate(rows):
        lat, lon, ts, speed = row
        gap = 0
        if i > 0:
            gap = ts - rows[i-1][2]
        result.append({
            "index": i,
            "lat": lat,
            "lon": lon,
            "time": ts_to_ist(ts).strftime("%H:%M:%S"),
            "timestamp": ts,
            "speed": speed,
            "gap_seconds": gap,
            "gap_minutes": round(gap / 60, 1),
            "BIG_GAP": gap > 600   # flag gaps > 10 minutes
        })

    total = len(rows)
    big_gaps = [r for r in result if r["BIG_GAP"]]

    return jsonify({
        "total_points": total,
        "big_gaps_count": len(big_gaps),
        "big_gaps": big_gaps,
        "all_points": result
    })

@app.route("/debug-count/<bus_no>/<path:departure_date>")
def debug_count(bus_no, departure_date):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # What /route fetches
    c.execute("""
        SELECT COUNT(*) FROM trip_points tp
        JOIN journeys j ON tp.journey_id = j.journey_id
        WHERE j.bus_no = ? AND j.departure_date = ?
    """, (bus_no, departure_date))
    route_count = c.fetchone()[0]

    # What /route-matched fetches
    c.execute("""
        SELECT COUNT(*) FROM trip_points tp
        JOIN journeys j ON tp.journey_id = j.journey_id
        WHERE j.bus_no = ? AND j.departure_date LIKE ?
    """, (bus_no, departure_date + "%"))
    matched_count = c.fetchone()[0]

    # How many journeys exist for this day
    c.execute("""
        SELECT journey_id, start_timestamp, end_timestamp, status,
               (SELECT COUNT(*) FROM trip_points WHERE journey_id = j.journey_id) as point_count
        FROM journeys j
        WHERE bus_no = ? AND departure_date = ?
        ORDER BY start_timestamp
    """, (bus_no, departure_date))
    journeys = [{"journey_id": r[0],
                 "start": ts_to_ist(r[1]).strftime("%H:%M:%S"),
                 "end": ts_to_ist(r[2]).strftime("%H:%M:%S") if r[2] else None,
                 "status": r[3],
                 "points": r[4]} for r in c.fetchall()]

    conn.close()

    return jsonify({
        "route_endpoint_count": route_count,
        "route_matched_endpoint_count": matched_count,
        "counts_match": route_count == matched_count,
        "journeys": journeys,
        "total_journeys": len(journeys)
    })

# ================= FIX SPLIT JOURNEYS API =================

@app.route("/fix-split-journeys", methods=["GET", "POST"])
def fix_split_journeys():
    """
    API to detect and merge overnight journeys that were incorrectly split at midnight.

    GET  /fix-split-journeys          → Preview only (dry run), shows what would be merged
    POST /fix-split-journeys          → Actually merges the split journeys in the database

    A split is detected when:
      - Journey A ended between 00:00 and 06:00 IST (midnight-cut window)
      - Journey B started on the next calendar day
      - The gap between end of A and start of B is less than 15 minutes
        (meaning it wasn't a real stop — just an artificial midnight cut)

    On merge:
      - All GPS points (trip_points) of Journey B are reassigned to Journey A
      - Journey A's end_timestamp is extended to cover Journey B's endpoint
      - Journey A inherits Journey B's status (active/ended)
      - Journey B is deleted from the journeys table
    """

    dry_run = (request.method == "GET")

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("""
        SELECT journey_id, bus_no, departure_date, start_timestamp, end_timestamp, status
        FROM journeys
        ORDER BY bus_no, start_timestamp
    """)
    all_journeys = c.fetchall()

    from collections import defaultdict
    by_bus = defaultdict(list)
    for j in all_journeys:
        by_bus[j["bus_no"]].append(dict(j))

    candidates = []
    for bus_no, journeys in by_bus.items():
        for i in range(len(journeys) - 1):
            a = journeys[i]
            b = journeys[i + 1]

            if a["end_timestamp"] is None:
                continue

            end_dt   = ts_to_ist(a["end_timestamp"])
            start_dt = ts_to_ist(b["start_timestamp"])
            gap_secs = b["start_timestamp"] - a["end_timestamp"]

            # Ended between midnight and 06:00 → likely a midnight-cut, not a real stop
            midnight_cut = (0 <= end_dt.hour < 6)
            next_day     = (a["departure_date"] != b["departure_date"])
            short_gap    = (gap_secs < 900)  # less than 15 minutes

            if midnight_cut and next_day and short_gap:
                candidates.append({
                    "keep_journey_id":  a["journey_id"],
                    "drop_journey_id":  b["journey_id"],
                    "bus_no":           bus_no,
                    "keep_date":        a["departure_date"],
                    "drop_date":        b["departure_date"],
                    "journey_a_ended":  end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "journey_b_started":start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "gap_seconds":      gap_secs,
                    "gap_minutes":      round(gap_secs / 60, 1)
                })

    if not candidates:
        conn.close()
        return jsonify({
            "status": "nothing_to_fix",
            "message": "No incorrectly split overnight journeys found.",
            "merged_count": 0
        })

    if dry_run:
        conn.close()
        return jsonify({
            "status": "preview",
            "message": f"Found {len(candidates)} split journey(s). Send a POST request to /fix-split-journeys to apply the merge.",
            "would_merge": candidates
        })

    # ---- Apply merges (POST only) ----
    merged = []
    for pair in candidates:
        keep_id = pair["keep_journey_id"]
        drop_id = pair["drop_journey_id"]

        # 1. Move all GPS points from the dropped journey into the kept journey
        c.execute("UPDATE trip_points SET journey_id=? WHERE journey_id=?", (keep_id, drop_id))

        # 2. Extend kept journey's end_timestamp and inherit status from dropped journey
        c.execute("SELECT end_timestamp, status FROM journeys WHERE journey_id=?", (drop_id,))
        drop_row = c.fetchone()
        if drop_row:
            c.execute("""
                UPDATE journeys
                SET end_timestamp=?, status=?
                WHERE journey_id=?
            """, (drop_row["end_timestamp"], drop_row["status"], keep_id))

        # 3. Delete the now-empty dropped journey
        c.execute("DELETE FROM journeys WHERE journey_id=?", (drop_id,))
        merged.append(pair)

    conn.commit()
    conn.close()

    return jsonify({
        "status":        "success",
        "message":       f"Merged {len(merged)} split overnight journey(s) successfully.",
        "merged_count":  len(merged),
        "merged_details": merged
    })

# ================= CLEANUP STALE JOURNEYS =================

@app.route("/end-stale-journeys", methods=["GET", "POST"])
def end_stale_journeys():
    """
    GET  → dry run: shows what would be ended, no changes made
    POST → actually ends all stale active journeys

    Stale = active journey whose last GPS ping was more than 1 hour ago.
    End timestamp is set to the time of the last actual ping, not now.
    """
    dry_run = (request.method == "GET")
    now = int(time.time())
    cutoff = now - 3600

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
        SELECT j.journey_id,
               j.bus_no,
               j.departure_date,
               j.start_timestamp,
               MAX(tp.timestamp) as last_ping,
               COUNT(tp.id)      as point_count
        FROM journeys j
        LEFT JOIN trip_points tp ON tp.journey_id = j.journey_id
        WHERE j.status = 'active'
        GROUP BY j.journey_id
        HAVING last_ping < ? OR last_ping IS NULL
        ORDER BY j.bus_no, j.start_timestamp
    """, (cutoff,))

    rows = c.fetchall()
    results = []

    for row in rows:
        journey_id, bus_no, dep_date, start_ts, last_ping, point_count = row
        last_ping_str = ts_to_ist(last_ping).strftime("%Y-%m-%d %H:%M:%S") if last_ping else "NO PINGS"
        idle_minutes  = round((now - last_ping) / 60) if last_ping else None
        end_ts        = last_ping if last_ping else start_ts

        results.append({
            "journey_id":   journey_id,
            "bus_no":       bus_no,
            "date":         dep_date,
            "started":      ts_to_ist(start_ts).strftime("%Y-%m-%d %H:%M:%S"),
            "last_ping":    last_ping_str,
            "idle_minutes": idle_minutes,
            "point_count":  point_count,
            "action":       "would_end" if dry_run else "ended"
        })

        if not dry_run:
            end_journey(journey_id, end_ts)
            print(f"[STALE END] {bus_no} / {dep_date} — idle {idle_minutes} min, {point_count} points")

    conn.close()

    return jsonify({
        "dry_run":     dry_run,
        "now_ist":     ts_to_ist(now).strftime("%Y-%m-%d %H:%M:%S"),
        "stale_count": len(results),
        "journeys":    results
    })

# ================= START =================
if __name__ == "__main__":
    init_db()
    threading.Thread(
        target=tracking_loop,
        daemon=True
    ).start()
    app.run(
        host="0.0.0.0",
        port=5000
    )
