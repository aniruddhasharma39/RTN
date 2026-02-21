import requests
import json
import os
import time
import threading
import random
import sqlite3
import websocket
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request

app = Flask(__name__)

API_URL = "https://reports.yourbus.in/ci/trackApp"
BUS_FILE = "buses.json"
DB_FILE = "/data/fleet.db"
OSRM_URL = "https://router.project-osrm.org"
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
    departure_date = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
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

                # End journey if date changed
                if active_journey:
                    conn = sqlite3.connect(DB_FILE)
                    c = conn.cursor()
                    c.execute(
                        "SELECT departure_date FROM journeys WHERE journey_id=?",
                        (active_journey,)
                    )
                    row = c.fetchone()
                    conn.close()

                    if row:
                        journey_date = row[0]
                        today = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
                        if journey_date != today:
                            end_journey(active_journey, timestamp)
                            active_journey = create_new_journey(bus_no, timestamp)
                            print("[NEW JOURNEY CREATED]", bus_no)

                state = fleet_state[bus_no]

                # ================= IDLE DETECTION =================

                last_loc = state.get("last_location")

                if last_loc:
                    movement = haversine(last_loc[0], last_loc[1], lat, lon)
                else:
                    movement = 0

                if movement <= 0.15:

                    if state["idle_start_time"] is None:
                        state["idle_start_time"] = timestamp
                        state["idle_start_location"] = (lat, lon)

                    idle_duration = timestamp - state["idle_start_time"]
                    distance = haversine(
                        state["idle_start_location"][0],
                        state["idle_start_location"][1],
                        lat, lon
                    )

                    if active_journey and idle_duration >= 3600 and distance <= 0.5:
                        print(f"[API JOURNEY END] {bus_no}")
                        end_journey(active_journey, timestamp)
                        active_journey = create_new_journey(bus_no, timestamp)
                        print(f"[API NEW JOURNEY CREATED] {bus_no}")
                        state["idle_start_time"] = None
                        state["idle_start_location"] = None

                else:
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
            "doj": datetime.now().strftime("%Y-%m-%d"),
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

            # ================= DATE CHANGE CHECK =================
            if active_journey:
                conn = sqlite3.connect(DB_FILE)
                c = conn.cursor()
                c.execute("""
                    SELECT departure_date
                    FROM journeys
                    WHERE journey_id=?
                """, (active_journey,))
                row = c.fetchone()
                conn.close()

                if row:
                    journey_date = row[0]
                    today = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
                    if journey_date != today:
                        end_journey(active_journey, timestamp)
                        active_journey = create_new_journey(bus_no, timestamp)
                        print("[NEW DAY JOURNEY]", bus_no)

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
                                    active_journey = create_new_journey(bus_no, timestamp)
                                    print("[WS NEW JOURNEY CREATED]", bus_no)
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

    distance = 0
    for i in range(1, len(rows)):
        distance += haversine(rows[i-1][0], rows[i-1][1], rows[i][0], rows[i][1])

    time_diff = end_ts - start_ts
    hours = time_diff // 3600
    minutes = (time_diff % 3600) // 60

    return jsonify({
        "distance_km": round(distance, 2),
        "hours": hours,
        "minutes": minutes
    })


# ================= OSRM MAP MATCHING =================

def match_points_osrm(rows):
    if len(rows) < 2:
        print("[OSRM] Not enough points:", len(rows))
        return []

    # Filter GPS jitter: skip points closer than 20 metres
    filtered = [rows[0]]
    for row in rows[1:]:
        last = filtered[-1]
        if haversine(last[0], last[1], row[0], row[1]) > 0.02:
            filtered.append(row)
    rows = filtered

    print(f"[OSRM] Points after jitter filter: {len(rows)}")

    if len(rows) < 2:
        return []

    matched_coords = []
    BATCH = 100

    for batch_start in range(0, len(rows), BATCH - 1):
        batch = rows[batch_start : batch_start + BATCH]
        if len(batch) < 2:
            continue

        coords = ";".join(f"{lon},{lat}" for lat, lon, ts in batch)
        timestamps = ";".join(str(ts) for lat, lon, ts in batch)
        radiuses = ";".join(["50"] * len(batch))

        url = f"{OSRM_URL}/match/v1/driving/{coords}"
        params = {
            "overview": "full",
            "geometries": "geojson",
            "timestamps": timestamps,
            "radiuses": radiuses,
            "gaps": "ignore"
        }

        try:
            print(f"[OSRM] Sending batch {batch_start}–{batch_start+len(batch)}")
            print(f"[OSRM] First point: {batch[0]}, Last point: {batch[-1]}")
            print(f"[OSRM] First timestamp: {batch[0][2]}, Last timestamp: {batch[-1][2]}")
            print(f"[OSRM] URL: {url}")
            
            res = requests.get(url, params=params, timeout=15)
            print(f"[OSRM] HTTP status: {res.status_code}")
            
            data = res.json()
            print(f"[OSRM] Full response: {json.dumps(data)[:500]}")  # first 500 chars
            
            if data.get("matchings"):
                for matching in data["matchings"]:
                    geometry = matching["geometry"]["coordinates"]
                    matched_coords.extend([[lat, lon] for lon, lat in geometry])
            else:
                print(f"[OSRM] code={data.get('code')} message={data.get('message','')}")
        
        except Exception as e:
            print(f"[OSRM] Exception: {e}")

    print(f"[OSRM] Total matched coords: {len(matched_coords)}")
    return matched_coords


@app.route("/route-matched/<bus_no>/<path:departure_date>")
def route_matched(bus_no, departure_date):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT lat, lon, timestamp
        FROM trip_points tp
        JOIN journeys j
        ON tp.journey_id=j.journey_id
        WHERE bus_no=?
        AND departure_date LIKE ?
        ORDER BY timestamp
    """, (bus_no, departure_date + "%"))
    rows = c.fetchall()
    conn.close()

    print(f"[ROUTE-MATCHED] {bus_no} {departure_date} → {len(rows)} raw points from DB")

    matched = match_points_osrm(rows)

    print(f"[ROUTE-MATCHED] Returning {len(matched)} matched coords")
    return jsonify(matched)

@app.route("/debug/<bus_no>/<path:departure_date>")
def debug_points(bus_no, departure_date):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT lat, lon, timestamp
        FROM trip_points tp
        JOIN journeys j ON tp.journey_id=j.journey_id
        WHERE bus_no=? AND departure_date LIKE ?
        ORDER BY timestamp
        LIMIT 10
    """, (bus_no, departure_date + "%"))
    rows = c.fetchall()
    conn.close()
    return jsonify([{
        "lat": r[0], "lon": r[1],
        "timestamp": r[2],
        "human_time": datetime.fromtimestamp(r[2]).strftime("%Y-%m-%d %H:%M:%S")
    } for r in rows])


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
