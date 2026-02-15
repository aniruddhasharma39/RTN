import requests
import json
import os
import time
import threading
import random
import sqlite3
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request

app = Flask(__name__)

API_URL = "https://reports.yourbus.in/ci/trackApp"
BUS_FILE = "buses.json"
DB_FILE = "fleet.db"
CHECK_INTERVAL = 10

STOP_THRESHOLD = 120          # 2 minutes
JOURNEY_END_THRESHOLD = 3600  # 1 hour
STABLE_RADIUS_KM = 0.5        # 500 meters
MOVEMENT_THRESHOLD = 5        # speed > 5 km/h means moving
IDLE_THRESHOLD = 120          # 2 minutes idle → LONG_IDLE
RESUME_DISTANCE_KM = 0.3      # must move at least 300m
END_CONFIRM_THRESHOLD = 3600  # 1 hour idle → end journey



fleet_state = {}


# ================= DATABASE =================

def init_db():
    conn = sqlite3.connect(DB_FILE)
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
    while True:
        try:
            with open(BUS_FILE) as f:
                buses = json.load(f)

            for bus in buses:
                print("Processing bus from JSON:", bus["bus_no"])
                bus_no = bus["bus_no"]
                if bus_no not in fleet_state:
                    fleet_state[bus_no] = {
                        "state": "ACTIVE",
                        "idle_start_time": None,
                        "idle_start_location": None,
                        "last_timestamp": None
                    }

                headers = {
                    "Content-Type": "application/json",
                    "Authentication": bus["auth"]
                }

                payload = {
                    "o": bus["operator"],
                    "v": bus["bus_no"],
                    "g": bus["device_id"]
                }

                try:
                    r = requests.post(API_URL, json=payload, headers=headers, timeout=5)
                    resp = r.json()
                    print("API Response for", bus_no, ":", resp.get("msg"))
                    if bus_no == "MP09AS9990":
                       print("FULL RESPONSE:", resp)

                    if resp.get("msg") not in ["Ok", "Stale"]:
                      continue

                    d = resp["data"]
                    lat = float(d["lt"])
                    lon = float(d["lg"])
                    print("Bus:", bus_no, "| Lat:", lat, "| Lon:", lon)

                    speed = float(d["sp"])
                    timestamp = int(time.time())
                    current_location = (lat, lon)

                    # ==========================================
                    # INITIALIZE FLEET STATE
                    # ==========================================
    

                    state_data = fleet_state[bus_no]
                    current_state = state_data["state"]

                    active_journey = get_active_journey(bus_no)
                    # ==========================================
                    # IF NO ACTIVE JOURNEY
                    # ==========================================
                    if not active_journey:
                        # TEMPORARY DEBUG VERSION (force create)
                        print("Creating journey for", bus_no)
                        active_journey = create_new_journey(bus_no, timestamp)
                        state_data["state"] = "ACTIVE"
                        state_data["idle_start_time"] = None
                        state_data["idle_start_location"] = None
                    # ==========================================
                    # INSERT TRIP POINT
                    # ==========================================
                    conn = sqlite3.connect(DB_FILE)
                    c = conn.cursor()
                    c.execute("""
                        INSERT INTO trip_points (journey_id, timestamp, lat, lon, speed)
                        VALUES (?, ?, ?, ?, ?)
                    """, (active_journey, timestamp, lat, lon, speed))
                    conn.commit()
                    conn.close()

                    # ==========================================
                    # FSM LOGIC
                    # ==========================================

                    # ------------------------------
                    # ACTIVE STATE
                    # ------------------------------
                    if current_state == "ACTIVE":

                        if speed <= 3:
                            if not state_data["idle_start_time"]:
                                state_data["idle_start_time"] = timestamp
                                state_data["idle_start_location"] = current_location
                            else:
                                idle_duration = timestamp - state_data["idle_start_time"]

                                if idle_duration >= IDLE_THRESHOLD:
                                    state_data["state"] = "LONG_IDLE"

                        else:
                            state_data["idle_start_time"] = None
                            state_data["idle_start_location"] = None


                    # ------------------------------
                    # LONG_IDLE STATE
                    # ------------------------------
                    elif current_state == "LONG_IDLE":

                        idle_loc = state_data["idle_start_location"]

                        distance_moved = haversine(
                            idle_loc[0], idle_loc[1],
                            lat, lon
                        )

                        idle_duration = timestamp - state_data["idle_start_time"]

                        # If vehicle resumes movement significantly
                        if speed > MOVEMENT_THRESHOLD and distance_moved > RESUME_DISTANCE_KM:
                            state_data["state"] = "ACTIVE"
                            state_data["idle_start_time"] = None
                            state_data["idle_start_location"] = None

                        # If still idle and exceeds full end threshold
                        elif idle_duration >= END_CONFIRM_THRESHOLD:
                            end_journey(active_journey, timestamp)
                            state_data["state"] = "ENDED"
                            state_data["idle_start_time"] = None
                            state_data["idle_start_location"] = None


                    # ------------------------------
                    # ENDED STATE
                    # ------------------------------
                    elif current_state == "ENDED":

                        # If vehicle starts moving again → new journey
                        if speed > MOVEMENT_THRESHOLD:
                            new_journey = create_new_journey(bus_no, timestamp)
                            state_data["state"] = "ACTIVE"
                            state_data["idle_start_time"] = None
                            state_data["idle_start_location"] = None
                            active_journey = new_journey


                    # ==========================================
                    # NETWORK FREEZE CHECK
                    # ==========================================
                    last_ts = state_data.get("last_timestamp")

                    if last_ts and (timestamp - last_ts >= END_CONFIRM_THRESHOLD):

                        if active_journey:
                            end_journey(active_journey, timestamp)
                            state_data["state"] = "ENDED"

                    state_data["last_timestamp"] = timestamp

                except Exception as e:
                                print("ERROR:", e)


            time.sleep(CHECK_INTERVAL)

        except Exception as e:
            time.sleep(CHECK_INTERVAL)

# ================= ROUTES =================

@app.route("/")
def home():
    return render_template("map.html")

@app.route("/buses")
def get_buses():
    with open(BUS_FILE) as f:
        buses = json.load(f)
    return jsonify([b["bus_no"] for b in buses])

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
