

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
CHECK_INTERVAL = 10

STOP_THRESHOLD = 120          # 2 minutes
JOURNEY_END_THRESHOLD = 3600  # 1 hour
STABLE_RADIUS_KM = 0.5        # 500 meters
MOVEMENT_THRESHOLD = 5        # speed > 5 km/h means moving
IDLE_THRESHOLD = 120          # 2 minutes idle → LONG_IDLE
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
        SELECT journey_id FROM journeys
        WHERE bus_no = ? AND status = 'active'
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

                tracking_type = bus.get(
                    "tracking_type",
                    "trackapp"
                )


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
                        "last_timestamp": None
                    }
                active_journey = get_active_journey(bus_no)

                state = fleet_state[bus_no]

                # ================= FIRST JOURNEY =================

                if not active_journey:

                    active_journey = create_new_journey(bus_no, timestamp)

                    state["idle_start_time"] = None
                    state["idle_start_location"] = None


                # ================= CHECK IDLE =================

                else:

                    if speed <= 3:

                        if state["idle_start_time"] is None:

                            state["idle_start_time"] = timestamp
                            state["idle_start_location"] = (lat, lon)

                        else:

                            idle_duration = timestamp - state["idle_start_time"]

                            distance = haversine(
                                state["idle_start_location"][0],
                                state["idle_start_location"][1],
                                lat,
                                lon
                            )

                            # 2 hour idle AND no movement
                            if idle_duration >= 7200 and distance <= 0.3:

                                print(f"[JOURNEY END] {bus_no}")

                                end_journey(active_journey, timestamp)

                                active_journey = create_new_journey(bus_no, timestamp)

                                state["idle_start_time"] = None
                                state["idle_start_location"] = None

                    else:

                        # vehicle moving → reset idle
                        state["idle_start_time"] = None
                        state["idle_start_location"] = None


                conn = sqlite3.connect(DB_FILE)
                c = conn.cursor()

                c.execute("""
                    INSERT INTO trip_points
                    (journey_id, timestamp, lat, lon, speed)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    active_journey,
                    timestamp,
                    lat,
                    lon,
                    speed
                ))

                conn.commit()
                conn.close()

                print(f"[API] {bus_no} → {lat},{lon}")


        except Exception as e:

            print("Tracking Error:", e)


        time.sleep(10)


def websocket_listener(bus):
    service_no = bus.get("serviceNo")

    # Use serviceNo as primary ID for websocket tracking
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
                print(f"[WS VEHICLE] {service_no} using vehicle {vehicle_number}")
    
            if not position:
                return
    
            lat = float(position["latitude"])
            lon = float(position["longitude"])
    
            speed = 0
            timestamp = int(time.time())
    
            # IMPORTANT: keep serviceNo as bus_no
            bus_no = service_no
    
            # ================= INIT STATE =================
    
            if bus_no not in fleet_state:
    
                fleet_state[bus_no] = {
                    "idle_start_time": None,
                    "idle_location": None,
                    "last_location": None
                }
    
            state = fleet_state[bus_no]
    
            active_journey = get_active_journey(bus_no)
            # Force new journey if database has none
            if active_journey is None:
            
                active_journey = create_new_journey(bus_no, timestamp)
            
                print(f"[NEW JOURNEY][WS] {bus_no}")
    
    
            if not active_journey:
    
                active_journey = create_new_journey(bus_no, timestamp)
    
                print(f"[NEW JOURNEY][WS] {bus_no}")
    
            else:
    
                last_location = state.get("last_location")
    
                if last_location:
    
                    distance = haversine(
                        last_location[0],
                        last_location[1],
                        lat,
                        lon
                    )
    
                    # vehicle stationary
                    if distance <= 0.05:
    
                        if state["idle_start_time"] is None:
    
                            state["idle_start_time"] = timestamp
                            state["idle_location"] = (lat, lon)
    
                    else:
    
                        # vehicle moved
    
                        if state["idle_start_time"]:
    
                            idle_duration = timestamp - state["idle_start_time"]
    
                            # idle ≥ 2 hr AND moved ≥ 5 km → new journey
                            restart_distance = haversine(
                                state["idle_location"][0],
                                state["idle_location"][1],
                                lat,
                                lon
                            )
    
                            if idle_duration >= 7200 and restart_distance >= 5:
    
                                print(f"[SERVICE RESTART][WS] {bus_no}")
    
                                end_journey(active_journey, timestamp)
    
                                active_journey = create_new_journey(bus_no, timestamp)
    
                                print(f"[NEW JOURNEY][WS] {bus_no}")
    
                        state["idle_start_time"] = None
                        state["idle_location"] = None
    
            state["last_location"] = (lat, lon)
    
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
    
            c.execute("""
                INSERT INTO trip_points
                (journey_id, timestamp, lat, lon, speed)
                VALUES (?, ?, ?, ?, ?)
            """, (
                active_journey,
                timestamp,
                lat,
                lon,
                speed
            ))
    
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

            result.append(b.get("serviceNo"))

        else:

            result.append(b.get("bus_no"))

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
        SELECT journey_id FROM journeys
        WHERE bus_no = ? AND departure_date = ?
    """, (bus_no, departure_date))
    journey_ids = [row[0] for row in c.fetchall()]

    all_points = []

    for jid in journey_ids:
        c.execute("""
            SELECT lat, lon, timestamp, speed
            FROM trip_points
            WHERE journey_id = ?
            ORDER BY timestamp
        """, (jid,))
        all_points.extend(c.fetchall())

    conn.close()
    return jsonify(all_points)

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

# ================= START =================

if __name__ == "__main__":
    init_db()
    threading.Thread(target=tracking_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
