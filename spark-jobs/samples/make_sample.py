import pandas as pd
from datetime import datetime, timedelta

START = datetime(2026, 10, 30, 8, 0, 0)   # UTC
N = 200
FMT = "%Y-%m-%dT%H:%M:%SZ"

# --- Positions ---
pos = []
for vid in (1, 2, 3):
    lat, lon, fuel = 33.57 + vid * 0.01, -7.59, 80.0
    for i in range(N):
        stopped = (vid == 3 and 50 <= i <= 130)      # arrêt de 40 min, moteur éteint
        if not stopped:
            lat += 0.0045                            # ~500 m / 30 s, soit ~60 km/h
            fuel -= 0.01
        if vid == 2 and i == 100:
            lat += 0.5                               # saut GPS (S29)
        if vid == 3 and 90 < i <= 110:
            fuel -= 1.25                             # vol : -25 points en 10 min (S30)
        pos.append({
            "vehicle_id": vid,
            "ts": (START + timedelta(seconds=30 * i)).strftime(FMT),
            "lat": round(lat, 6), "lon": round(lon, 6),
            "speed": 0.0 if stopped else 60.0, "heading": 0,
            "fuel_level": round(fuel, 2),
            "engine_on": 0 if stopped else 1,
        })
pd.DataFrame(pos).to_csv("data/csv/sample_positions.csv", index=False)

# --- Missions (une par véhicule, couvrant toute la période) ---
end = START + timedelta(seconds=30 * (N - 1))
pd.DataFrame([{
    "mission_id": 1000 + v, "vehicle_id": v, "driver_id": 100 + v, "depot_id": 1,
    "start_ts": START.strftime(FMT), "end_ts": end.strftime(FMT),
} for v in (1, 2, 3)]).to_csv("data/csv/sample_missions.csv", index=False)

# --- Événements de conduite ---
plan = [(1, 101, "speeding", 3), (1, 101, "harsh_braking", 1),
        (2, 102, "speeding", 1),
        (3, 103, "speeding", 6), (3, 103, "harsh_braking", 4), (3, 103, "idle", 2)]
events = []
for vid, did, typ, n in plan:
    for k in range(n):
        events.append({"vehicle_id": vid, "driver_id": did, "type": typ,
                       "ts": (START + timedelta(minutes=5 * k + 1)).strftime(FMT),
                       "value": 1.0})
pd.DataFrame(events).to_csv("data/csv/sample_events.csv", index=False)

print("OK", len(pos), "positions,", len(events), "événements")