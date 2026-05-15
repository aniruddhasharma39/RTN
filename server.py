import requests
import json
import os
import time
import threading
import random
import websocket
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request
from pymongo import MongoClient

app = Flask(__name__)

API_URL = "https://reports.yourbus.in/ci/trackApp"
BUS_FILE = "buses.json"
OSRM_URL = "https://router.project-osrm.org"
CHECK_INTERVAL = 10

STOP_THRESHOLD = 120          # 2 minutes
JOURNEY_END_THRESHOLD = 3600  # 1 hour
STABLE_RADIUS_KM = 0.5        # 500 meters
MOVEMENT_THRESHOLD = 5        # speed > 5 km/h means moving
IDLE_THRESHOLD = 120          # 2 minutes idle → LONG_IDLE
RESUME_DISTANCE_KM = 0.3      # must move at least 300m
END_CONFIRM_THRESHOLD = 3600  # 1 hour idle → end journey
BUSES_FILE = "buses.json"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(MONGO_URI)
db = client['fleet_db']
journeys_col = db['journeys']
trip_points_col = db['trip_points']

ws_started = {}
fleet_state = {}
service_vehicle_map = {}

def load_buses():
    try:
        with open(BUSES_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        print("Error loading buses.json:", e)
        return []

def init_db():
    journeys_col.create_index("bus_no")
    journeys_col.create_index("journey_id", unique=True)
    trip_points_col.create_index("journey_id")
    trip_points_col.create_index("timestamp")
    print("MongoDB connection established and indexes created.")

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

def generate_journey_id(bus_no):
    return f"{bus_no}_{int(time.time())}"

def get_active_journey(bus_no):
    journey = journeys_col.find_one(
        {"bus_no": bus_no, "status": "active"},
        sort=[("start_timestamp", -1)]
    )
    return journey["journey_id"] if journey else None

def create_new_journey(bus_no, timestamp):
    journey_id = generate_journey_id(bus_no)
    departure_date = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
    journeys_col.insert_one({
        "_id": journey_id,
        "journey_id": journey_id,
        "bus_no": bus_no,
        "departure_date": departure_date,
        "start_timestamp": timestamp,
        "status": "active"
    })
    return journey_id

def end_journey(journey_id, timestamp):
    journeys_col.update_one(
        {"journey_id": journey_id},
        {"$set": {"status": "ended", "end_timestamp": timestamp}}
    )

def process_bus_location(bus_no, tracking_type, lat, lon, speed, timestamp):
    """
    Common logic used by both API and Websocket to determine journey states.
    Stoppage for 1 hour without movement >= 300m results in ending the journey.
    """
    if bus_no not in fleet_state:
        fleet_state[bus_no] = {
            "state": "ACTIVE",
            "idle_start_time": None,
            "idle_location": None,
            "last_location": None,
            "last_signal_time": timestamp
        }

    state = fleet_state[bus_no]
    active_journey = get_active_journey(bus_no)

    if state.get("last_signal_time"):
        signal_gap = timestamp - state["last_signal_time"]
        if signal_gap >= 3600:
            print(f"[GPS LOSS END JOURNEY] {bus_no}")
            if active_journey:
                end_journey(active_journey, timestamp)
                active_journey = None
                state["idle_start_time"] = None
                state["idle_location"] = None

    last_loc = state.get("last_location")
    
    # Noise Filtering: Reject unrealistic GPS jumps (e.g. >150 km/h)
    if last_loc is not None and state.get("last_signal_time"):
        signal_gap = timestamp - state["last_signal_time"]
        if signal_gap > 0:
            dist = haversine(last_loc[0], last_loc[1], lat, lon)
            speed_kmh = dist / (signal_gap / 3600.0)
            if speed_kmh > 150:
                print(f"[GPS NOISE FILTERED] {bus_no} jump: {speed_kmh:.1f} km/h")
                return # Ignore this bad point completely

    state["last_signal_time"] = timestamp

    if last_loc is None:
        state["last_location"] = (lat, lon)
        print(f"[{tracking_type}] {bus_no} initialized → {lat},{lon}")
        if active_journey:
            trip_points_col.insert_one({
                "journey_id": active_journey,
                "timestamp": timestamp,
                "lat": lat,
                "lon": lon,
                "speed": speed
            })
        return

    movement = haversine(last_loc[0], last_loc[1], lat, lon)

    # 1. Active journey logic (Running bus or recently parked)
    if active_journey:
        if movement <= 0.05:
            if state["idle_start_time"] is None:
                state["idle_start_time"] = timestamp
                state["idle_location"] = (lat, lon)
            else:
                idle_duration = timestamp - state["idle_start_time"]
                idle_distance = haversine(state["idle_location"][0], state["idle_location"][1], lat, lon)
                
                # End journey criteria
                if idle_duration >= JOURNEY_END_THRESHOLD and idle_distance <= RESUME_DISTANCE_KM:
                    print(f"[JOURNEY END] {bus_no}")
                    end_journey(active_journey, timestamp)
                    active_journey = None  # Do not immediately create new journey
                    state["idle_start_time"] = None
                    state["idle_location"] = None
        else:
            state["idle_start_time"] = None
            state["idle_location"] = None

    # 2. No Active Journey Logic (Parked bus -> moving again)
    elif movement > 0.05:
        active_journey = create_new_journey(bus_no, timestamp)
        print(f"[NEW JOURNEY STARTED] {bus_no}")
        state["idle_start_time"] = None
        state["idle_location"] = None

    state["last_location"] = (lat, lon)

    # Insert Trip Point ONLY if there is an active journey
    if active_journey:
        trip_points_col.insert_one({
            "journey_id": active_journey,
            "timestamp": timestamp,
            "lat": lat,
            "lon": lon,
            "speed": speed
        })

    print(f"[{tracking_type}] {bus_no} → {lat},{lon}")


# ================= TRACKING LOOP =================
def tracking_loop():
    print("Tracking loop started")
    while True:
        try:
            buses = load_buses()
            for bus in buses:
                tracking_type = bus.get("tracking_type", "trackapp")
                
                if tracking_type == "websocket":
                    service_no = bus.get("serviceNo")
                    if not service_no: continue
                    if service_no not in ws_started:
                        ws_started[service_no] = True
                        threading.Thread(target=websocket_listener, args=(bus,), daemon=True).start()
                    continue

                # TRACKAPP BUSES (API)
                bus_no = bus.get("bus_no")
                device_id = bus.get("device_id")
                auth = bus.get("auth")

                if not device_id or not auth or not bus_no:
                    continue

                headers = {
                    "Content-Type": "application/json",
                    "Authentication": auth
                }
                payload = {
                    "o": bus.get("operator", ""),
                    "v": bus_no,
                    "g": device_id
                }
                
                try:
                    response = requests.post(API_URL, headers=headers, json=payload, timeout=10)
                    data = response.json()
                    if data["msg"] != "Ok": continue
                    
                    gps = data["data"]
                    lat = float(gps["lt"])
                    lon = float(gps["lg"])
                    speed = float(gps["sp"])
                    timestamp = int(time.time())

                    process_bus_location(bus_no, "API", lat, lon, speed, timestamp)
                except Exception as req_ex:
                    pass
        except Exception as e:
            print("Tracking Error:", e)
            
        time.sleep(10)


def websocket_listener(bus):
    service_no = bus.get("serviceNo")
    if not service_no:
        print(f"[WS] Skipped, no serviceNo")
        return

    def on_open(ws):
        now = datetime.now()
        if now.hour < 12:
            # Reconnecting in the early morning for a bus that departed yesterday
            doj_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            doj_str = now.strftime("%Y-%m-%d")
            
        payload = {
            "serviceNo": service_no,
            "doj": doj_str,
            "trackingType": "full-tracking"
        }
        ws.send(json.dumps(payload))
        print(f"[WS] Connected → {service_no} (DOJ: {doj_str})")

    def on_message(ws, message):
        try:
            data = json.loads(message)
            if not data.get("success"): return

            position = data.get("vehicleInfo", {}).get("position", {})
            vehicle_number = data.get("vehicleInfo", {}).get("registrationNumber")
            
            # Map tracking identifier (bus_no logic)
            if vehicle_number:
                service_vehicle_map[service_no] = vehicle_number
            
            # Use dynamic vehicle registration number to identify route, fallback to serviceNo
            bus_no = vehicle_number if vehicle_number else service_no

            if not position: return

            lat = float(position["latitude"])
            lon = float(position["longitude"])
            speed = 0
            timestamp = int(time.time())

            process_bus_location(bus_no, "WS", lat, lon, speed, timestamp)
            
        except Exception as e:
            print("[WS ERROR]", e)

    def reconnect():
        time.sleep(5)
        websocket_listener(bus)

    def on_close(ws, close_status_code, close_msg):
        print(f"[WS CLOSED] {service_no}")
        threading.Thread(target=reconnect, daemon=True).start()

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
    buses = load_buses()
    result = []
    # Distinct set of active vehicle numbers
    for b in buses:
        if b.get("tracking_type") == "websocket":
            service = b.get("serviceNo")
            vehicle = service_vehicle_map.get(service)
            bus_no = vehicle if vehicle else service
            result.append({
                "id": bus_no,
                "label": vehicle if vehicle else service
            })
        else:
            bus_no = b.get("bus_no")
            if bus_no:
                result.append({
                    "id": bus_no,
                    "label": bus_no
                })
                
    # Remove duplicates
    unique_result = {x['id']: x for x in result}.values()
    return jsonify(list(unique_result))


@app.route("/dates/<bus_no>")
def get_dates(bus_no):
    dates = journeys_col.distinct("departure_date", {"bus_no": bus_no})
    return jsonify(sorted(dates, reverse=True))

@app.route("/all-dates")
def get_all_dates():
    dates = journeys_col.distinct("departure_date")
    return jsonify(sorted(dates, reverse=True))


@app.route("/route/<bus_no>/<path:departure_date>")
def get_route(bus_no, departure_date):
    journeys = list(journeys_col.find(
        {"bus_no": bus_no, "departure_date": departure_date}
    ).sort("start_timestamp", 1))

    all_points = []
    if journeys:
        last_status = journeys[-1].get("status", "ended")
        ended = (last_status == "ended")
    else:
        ended = False

    for j in journeys:
        points = trip_points_col.find({"journey_id": j["journey_id"]}).sort("timestamp", 1)
        for p in points:
            all_points.append([p["lat"], p["lon"], p["timestamp"], p.get("speed", 0)])

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

    journeys = list(journeys_col.find({"bus_no": bus_no, "departure_date": departure_date}))
    j_ids = [j["journey_id"] for j in journeys]

    points = list(trip_points_col.find(
        {"journey_id": {"$in": j_ids}, "timestamp": {"$gte": start_ts, "$lte": end_ts}}
    ).sort("timestamp", 1))

    distance = 0
    for i in range(1, len(points)):
        distance += haversine(points[i-1]["lat"], points[i-1]["lon"], points[i]["lat"], points[i]["lon"])

    time_diff = end_ts - start_ts
    hours = time_diff // 3600
    minutes = (time_diff % 3600) // 60

    return jsonify({
        "distance_km": round(distance, 2),
        "hours": hours,
        "minutes": minutes
    })

def filter_unrealistic_points(rows):
    if not rows: return []
    filtered = [rows[0]]
    for i in range(1, len(rows)):
        prev = filtered[-1]
        curr = rows[i]
        
        dist = haversine(prev[0], prev[1], curr[0], curr[1])
        time_diff = curr[2] - prev[2]
        
        # OSRM requires monotonically increasing timestamps if used, but we'll 
        # also use this to filter out zero-time jumps.
        if time_diff <= 0:
            continue
            
        speed = dist / (time_diff / 3600.0) if time_diff > 0 else 0
        if speed < 150:  # Reasonableness check
            filtered.append(curr)
            
    return filtered

def match_points_osrm(rows):
    rows = filter_unrealistic_points(rows)
    if not rows:
        return []

    CHUNK_SIZE = 100
    all_coords = []
    
    for i in range(0, len(rows)-1, CHUNK_SIZE-1):
        chunk = rows[i:i+CHUNK_SIZE]
        if len(chunk) < 2:
            continue
            
        coords = ";".join([f"{r[1]},{r[0]}" for r in chunk])
        
        # Omit timestamps to allow OSRM to map-match without strict time logic
        # which often fails with noisy or duplicate GPS timestamps.
        url = f"{OSRM_URL}/match/v1/driving/{coords}"
        params = {
            "overview": "full",
            "geometries": "geojson",
            "radiuses": ";".join(["100"] * len(chunk))
        }
        
        # Implement retry logic for OSRM rate limiting
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params)
                if response.status_code == 429:
                    print(f"OSRM Rate limited. Retrying in 1 second... (Attempt {attempt+1}/{max_retries})")
                    time.sleep(1)
                    continue
                    
                data = response.json()
                
                if "matchings" in data and len(data["matchings"]) > 0:
                    # Get the best matching
                    match = max(data["matchings"], key=lambda m: m.get("confidence", 0))
                    geometry = match.get("geometry", {}).get("coordinates", [])
                    all_coords.extend([[lat, lon] for lon, lat in geometry])
                else:
                    # Map matching failed for this segment, use raw points
                    all_coords.extend([[r[0], r[1]] for r in chunk])
                break # Exit retry loop on success
                
            except Exception as e:
                print("OSRM error:", e)
                if attempt == max_retries - 1:
                    all_coords.extend([[r[0], r[1]] for r in chunk])
                else:
                    time.sleep(1)

    return all_coords

@app.route("/route-matched/<bus_no>/<departure_date>")
def route_matched(bus_no, departure_date):
    journeys = list(journeys_col.find({
        "bus_no": bus_no,
        "departure_date": {"$regex": f"^{departure_date}"}
    }))
    j_ids = [j["journey_id"] for j in journeys]

    points = list(trip_points_col.find({"journey_id": {"$in": j_ids}}).sort("timestamp", 1))
    rows = [[p["lat"], p["lon"], p["timestamp"]] for p in points]
    
    matched = match_points_osrm(rows)
    return jsonify(matched)

@app.route("/export-data")
def export_data():
    journeys = list(journeys_col.find({}, {"_id": 0}))
    return jsonify({"journeys_count": len(journeys), "journeys": journeys})

if __name__ == "__main__":
    try:
        init_db()
    except Exception as e:
        print(f"[STARTUP ERROR] MongoDB connection failed: {e}")
        print(f"[STARTUP ERROR] MONGO_URI = {os.getenv('MONGO_URI', 'NOT SET')}")
    threading.Thread(target=tracking_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    print(f"[STARTUP] Starting server on port {port}")
    app.run(host="0.0.0.0", port=port)
