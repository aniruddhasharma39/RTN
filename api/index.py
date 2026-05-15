import sys
import os

# Add parent directory to path so we can import from server.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, render_template, jsonify, request
from pymongo import MongoClient
import json
import time
import os
from math import radians, sin, cos, sqrt, atan2
import requests

app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates"),
    static_folder=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static"),
)

# ---- Config ----
BUSES_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "buses.json")
OSRM_URL = "https://router.project-osrm.org"

MONGO_URI = os.environ.get("MONGO_URI", "")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable is not set.")

client = MongoClient(MONGO_URI)
db = client["fleet_db"]
journeys_col = db["journeys"]
trip_points_col = db["trip_points"]

service_vehicle_map = {}

# ---- Helpers ----
def load_buses():
    try:
        with open(BUSES_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        print("Error loading buses.json:", e)
        return []

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

def filter_unrealistic_points(rows):
    if not rows:
        return []
    filtered = [rows[0]]
    for i in range(1, len(rows)):
        prev = filtered[-1]
        curr = rows[i]
        dist = haversine(prev[0], prev[1], curr[0], curr[1])
        time_diff = curr[2] - prev[2]
        if time_diff <= 0:
            continue
        speed = dist / (time_diff / 3600.0) if time_diff > 0 else 0
        if speed < 150:
            filtered.append(curr)
    return filtered

def match_points_osrm(rows):
    rows = filter_unrealistic_points(rows)
    if not rows:
        return []
    CHUNK_SIZE = 100
    all_coords = []
    for i in range(0, len(rows) - 1, CHUNK_SIZE - 1):
        chunk = rows[i : i + CHUNK_SIZE]
        if len(chunk) < 2:
            continue
        coords = ";".join([f"{r[1]},{r[0]}" for r in chunk])
        url = f"{OSRM_URL}/match/v1/driving/{coords}"
        params = {
            "overview": "full",
            "geometries": "geojson",
            "radiuses": ";".join(["100"] * len(chunk)),
        }
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 429:
                    time.sleep(1)
                    continue
                data = response.json()
                if "matchings" in data and len(data["matchings"]) > 0:
                    match = max(data["matchings"], key=lambda m: m.get("confidence", 0))
                    geometry = match.get("geometry", {}).get("coordinates", [])
                    all_coords.extend([[lat, lon] for lon, lat in geometry])
                else:
                    all_coords.extend([[r[0], r[1]] for r in chunk])
                break
            except Exception as e:
                print("OSRM error:", e)
                if attempt == max_retries - 1:
                    all_coords.extend([[r[0], r[1]] for r in chunk])
                else:
                    time.sleep(1)
    return all_coords

# ---- Routes ----
@app.route("/")
def home():
    return render_template("map.html")

@app.route("/buses")
def get_buses():
    buses = load_buses()
    result = []
    for b in buses:
        if b.get("tracking_type") == "websocket":
            service = b.get("serviceNo")
            vehicle = service_vehicle_map.get(service)
            bus_no = vehicle if vehicle else service
            result.append({"id": bus_no, "label": vehicle if vehicle else service})
        else:
            bus_no = b.get("bus_no")
            if bus_no:
                result.append({"id": bus_no, "label": bus_no})
    unique_result = {x["id"]: x for x in result}.values()
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
    journeys = list(
        journeys_col.find({"bus_no": bus_no, "departure_date": departure_date}).sort("start_timestamp", 1)
    )
    all_points = []
    if journeys:
        last_status = journeys[-1].get("status", "ended")
        ended = last_status == "ended"
    else:
        ended = False
    for j in journeys:
        points = trip_points_col.find({"journey_id": j["journey_id"]}).sort("timestamp", 1)
        for p in points:
            all_points.append([p["lat"], p["lon"], p["timestamp"], p.get("speed", 0)])
    return jsonify({"points": all_points, "ended": ended})

@app.route("/measure", methods=["POST"])
def measure():
    data = request.json
    bus_no = data["bus_no"]
    departure_date = data["trip_date"]
    start_ts = data["start_ts"]
    end_ts = data["end_ts"]
    journeys = list(journeys_col.find({"bus_no": bus_no, "departure_date": departure_date}))
    j_ids = [j["journey_id"] for j in journeys]
    points = list(
        trip_points_col.find(
            {"journey_id": {"$in": j_ids}, "timestamp": {"$gte": start_ts, "$lte": end_ts}}
        ).sort("timestamp", 1)
    )
    distance = 0
    for i in range(1, len(points)):
        distance += haversine(
            points[i - 1]["lat"], points[i - 1]["lon"], points[i]["lat"], points[i]["lon"]
        )
    time_diff = end_ts - start_ts
    hours = time_diff // 3600
    minutes = (time_diff % 3600) // 60
    return jsonify({"distance_km": round(distance, 2), "hours": hours, "minutes": minutes})

@app.route("/route-matched/<bus_no>/<departure_date>")
def route_matched(bus_no, departure_date):
    journeys = list(
        journeys_col.find({"bus_no": bus_no, "departure_date": {"$regex": f"^{departure_date}"}})
    )
    j_ids = [j["journey_id"] for j in journeys]
    points = list(trip_points_col.find({"journey_id": {"$in": j_ids}}).sort("timestamp", 1))
    rows = [[p["lat"], p["lon"], p["timestamp"]] for p in points]
    matched = match_points_osrm(rows)
    return jsonify(matched)

@app.route("/export-data")
def export_data():
    journeys = list(journeys_col.find({}, {"_id": 0}))
    return jsonify({"journeys_count": len(journeys), "journeys": journeys})
