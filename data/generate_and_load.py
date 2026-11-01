#!/usr/bin/env python3
"""Générateur + chargeur de données de la flotte (rôle A).

Depuis la racine du dépôt :
  python -m data.generate_and_load --dry-run                 # génère, n'écrit rien
  python -m data.generate_and_load --scale 0.1 --cl ONE      # petit jeu (dev)
  python -m data.generate_and_load --cl QUORUM               # jeu complet (full)

Pour que A et B obtiennent EXACTEMENT les mêmes données, utilisez la même
graine ET la même date de référence :
  python -m data.generate_and_load --seed 42 --now 2026-10-31T12:00:00

Anomalies injectées (vérité terrain dans data/ground_truth.json, pour valider S28-S30) :
  - arrêts non planifiés > 30 min pendant une mission        (S28)
  - sauts GPS impliquant plus de 200 km/h                    (S29)
  - baisses de carburant > 20 % en 10 min, moteur éteint     (S30)
"""
import argparse
import datetime as dt
import json
import math
import os
import random
import uuid
from collections import defaultdict, namedtuple
from decimal import Decimal

import numpy as np
import pygeohash as pgh
from faker import Faker
from tqdm import tqdm

# --- Types UDT (l'ordre des champs doit suivre schema.cql) -------------------
GeoPoint = namedtuple("GeoPoint", "lat lon")
Place = namedtuple("Place", "name address lat lon geohash")
PartItem = namedtuple("PartItem", "name reference qty unit_cost")

# --- Paramètres du monde simulé ---------------------------------------------
DEPOTS = [  # id, nom, ville, lat, lon, poids (part de la flotte)
    ("DEP01", "Dépôt Casablanca", "Casablanca", 33.5731, -7.5898, 0.26),
    ("DEP02", "Dépôt Rabat", "Rabat", 34.0209, -6.8416, 0.14),
    ("DEP03", "Dépôt Marrakech", "Marrakech", 31.6295, -7.9811, 0.13),
    ("DEP04", "Dépôt Tanger", "Tanger", 35.7595, -5.8340, 0.12),
    ("DEP05", "Dépôt Fès", "Fès", 34.0181, -5.0078, 0.10),
    ("DEP06", "Dépôt Agadir", "Agadir", 30.4278, -9.5981, 0.09),
    ("DEP07", "Dépôt El Jadida", "El Jadida", 33.2316, -8.5007, 0.08),
    ("DEP08", "Dépôt Meknès", "Meknès", 33.8935, -5.5547, 0.08),
]
EXTRA_CITIES = [("Oujda", 34.6867, -1.9114), ("Kénitra", 34.2610, -6.5802),
                ("Safi", 32.2994, -9.2372), ("Béni Mellal", 32.3373, -6.3498),
                ("Tétouan", 35.5785, -5.3684), ("Settat", 33.0010, -7.6166),
                ("Nador", 35.1681, -2.9335)]
CITIES = [(d[2], d[3], d[4]) for d in DEPOTS] + EXTRA_CITIES

VEHICLE_CATALOG = {  # type -> [(marque, modèle, capacité kg)]
    "Camionnette": [("Renault", "Kangoo", 800), ("Peugeot", "Partner", 750), ("Citroën", "Berlingo", 750)],
    "Fourgon": [("Renault", "Master", 1500), ("Ford", "Transit", 1400), ("Mercedes", "Sprinter", 1600)],
    "Camion": [("Isuzu", "NPR", 3500), ("Mercedes", "Atego", 8000), ("Renault", "D Wide", 9000)],
    "Semi-remorque": [("Volvo", "FH", 24000), ("Scania", "R450", 25000), ("DAF", "XF", 24500)],
}
TYPE_WEIGHTS = [0.30, 0.30, 0.25, 0.15]
EQUIP_OPT = ["FRIGO", "HAYON", "CAMERA_RECUL", "LIMITEUR_VITESSE", "ABS", "CLIM", "ATTELAGE"]
EVENT_TYPES = ["SPEEDING", "HARSH_BRAKING", "LONG_IDLE", "GEOFENCE_EXIT"]

MAINT_CATALOG = {  # type -> pièces (nom, référence, quantité, prix unitaire MAD)
    "VIDANGE": [("Huile moteur 15W40", "OIL-15W40", 1, "650"), ("Filtre à huile", "FLT-OIL", 1, "90"),
                ("Filtre à air", "FLT-AIR", 1, "120")],
    "PNEUS": [("Pneu 205/75R16", "TYR-205", 4, "900")],
    "FREINS": [("Plaquettes avant", "BRK-PAD-F", 1, "450"), ("Disques avant", "BRK-DSK-F", 2, "600")],
    "COURROIE": [("Kit distribution", "TIM-KIT", 1, "1800")],
    "BATTERIE": [("Batterie 100Ah", "BAT-100", 1, "1400")],
    "CONTROLE_TECHNIQUE": [],
}

STOP_PROB = 0.06     # part des trajets avec un arrêt non planifié > 30 min
THEFT_SHARE = 0.35   # part de ces arrêts avec une baisse suspecte de carburant
JUMP_PROB = 0.0004   # probabilité d'un saut GPS par point
FUEL_PCT_PER_KM = 0.09
R_EARTH = 6371.0088


# --- Utilitaires géographiques ----------------------------------------------
def clip(x, lo, hi):
    return max(lo, min(hi, x))


def haversine(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R_EARTH * math.asin(math.sqrt(a))


def bearing(lat1, lon1, lat2, lon2):
    p1, p2, dl = math.radians(lat1), math.radians(lat2), math.radians(lon2 - lon1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def make_place(name, address, lat, lon):
    return Place(name, address, round(lat, 5), round(lon, 5), pgh.encode(lat, lon, precision=7))


# --- Référentiels ------------------------------------------------------------
def gen_vehicles(n, rng):
    types = list(VEHICLE_CATALOG)
    depot_ids, dweights = [d[0] for d in DEPOTS], [d[5] for d in DEPOTS]
    plates, out = set(), []
    for _ in range(n):
        while True:
            plate = f"{rng.randint(1, 99999):05d}-{rng.choice('ABDEHJTW')}-{rng.randint(1, 89)}"
            if plate not in plates:
                plates.add(plate)
                break
        vtype = rng.choices(types, TYPE_WEIGHTS)[0]
        brand, model, cap = rng.choice(VEHICLE_CATALOG[vtype])
        out.append(dict(
            plate=plate, vehicle_type=vtype, brand=brand, model=model,
            year=rng.randint(2012, 2025), capacity_kg=cap,
            depot_id=rng.choices(depot_ids, dweights)[0],
            equipment={"GPS"} | set(rng.sample(EQUIP_OPT, rng.randint(1, 4))),
            features={"carburant": "diesel", "boite": rng.choice(["manuelle", "automatique"]),
                      "norme": rng.choice(["Euro 5", "Euro 6"]),
                      "couleur": rng.choice(["blanc", "gris", "bleu", "rouge"]),
                      "boitier_gps": f"TK-{rng.randint(1000, 9999)}"},
            last_maint=None))
    return out


def gen_drivers(n, rng, nrng, fake):
    depot_ids, dweights = [d[0] for d in DEPOTS], [d[5] for d in DEPOTS]
    licenses = [["B"], ["B", "C"], ["B", "C", "CE"], ["B", "D"], ["B", "C", "CE", "D"]]
    lw = [0.15, 0.30, 0.30, 0.10, 0.15]
    # Propension individuelle aux excès de vitesse / freinages brusques (moyenne 1)
    ps = nrng.lognormal(0, 0.7, n)
    pb = nrng.lognormal(0, 0.5, n)
    ps, pb = ps / ps.mean(), pb / pb.mean()
    start, span = dt.date(2008, 1, 1), (dt.date(2026, 6, 30) - dt.date(2008, 1, 1)).days
    return [dict(driver_id=f"DRV{i + 1:05d}", name=fake.name(),
                 license_categories=rng.choices(licenses, lw)[0],
                 hire_date=start + dt.timedelta(days=rng.randint(0, span)),
                 depot_id=rng.choices(depot_ids, dweights)[0],
                 ps=float(ps[i]), pb=float(pb[i])) for i in range(n)]


# --- Missions ----------------------------------------------------------------
def pick_destination(o_lat, o_lon, home_city, rng, fake):
    if rng.random() < 0.35:  # livraison locale
        lat, lon = o_lat + rng.uniform(-0.15, 0.15), o_lon + rng.uniform(-0.15, 0.15)
        city = home_city
    else:
        cands = [c for c in CITIES if c[0] != home_city
                 and 40 <= haversine(o_lat, o_lon, c[1], c[2]) <= 450]
        if cands:
            city, clat, clon = rng.choice(cands)
            lat, lon = clat + rng.uniform(-0.05, 0.05), clon + rng.uniform(-0.05, 0.05)
        else:  # aucune ville dans la plage : livraison locale
            city = home_city
            lat, lon = o_lat + rng.uniform(-0.15, 0.15), o_lon + rng.uniform(-0.15, 0.15)
    return make_place(f"Client {fake.company()} ({city})", fake.street_address(), lat, lon)


def gen_missions(n, n_live, vehicles, drivers, now, rng, nrng, fake, depot_places, depot_city, speed_factor):
    by_depot = defaultdict(list)
    for d in drivers:
        by_depot[d["depot_id"]].append(d)
    busy_v, busy_d = defaultdict(list), defaultdict(list)

    def is_free(book, key, s, e):
        return all(e <= a or s >= b for a, b in book[key])

    missions, attempts = [], 0
    while len(missions) < n and attempts < n * 30:
        attempts += 1
        live = len(missions) < n_live
        v = rng.choice(vehicles)
        dep = v["depot_id"]
        o = depot_places[dep]
        dest = pick_destination(o.lat, o.lon, depot_city[dep], rng, fake)
        dist = max(3.0, haversine(o.lat, o.lon, dest.lat, dest.lon))
        if live:
            p_start = now - dt.timedelta(minutes=rng.randint(20, 120))
        else:
            day = now.date() - dt.timedelta(days=rng.randint(-2, 29))
            p_start = dt.datetime.combine(day, dt.time()) + dt.timedelta(minutes=rng.randint(300, 1020))
        p_end = p_start + dt.timedelta(minutes=dist / 48 * 60 + 45)
        a_start = p_start + dt.timedelta(minutes=int(nrng.normal(6, 10)))
        speed = clip(nrng.normal(52 * speed_factor[dep], 8), 30, 85)
        load = clip(nrng.normal(25, 8), 5, 60)
        a_end = a_start + dt.timedelta(minutes=dist / speed * 60 + load)
        if live and a_end <= now:
            a_end = now + dt.timedelta(minutes=rng.randint(10, 90))
        drv = rng.choice(by_depot.get(dep) or drivers)
        window = (a_start - dt.timedelta(minutes=15), max(a_end, p_end) + dt.timedelta(minutes=60))
        if not (is_free(busy_v, v["plate"], *window) and is_free(busy_d, drv["driver_id"], *window)):
            continue
        busy_v[v["plate"]].append(window)
        busy_d[drv["driver_id"]].append(window)

        if not live and rng.random() < 0.02:
            status = "CANCELLED"
        elif a_end <= now:
            status = "DONE"
        elif a_start <= now:
            status = "IN_PROGRESS"
        else:
            status = "PLANNED"
        missions.append(dict(
            mission_id=uuid.UUID(int=rng.getrandbits(128), version=4),
            plate=v["plate"], driver_id=drv["driver_id"], depot_id=dep,
            origin=o, destination=dest, planned_start=p_start, planned_end=p_end,
            actual_start=a_start if status in ("DONE", "IN_PROGRESS") else None,
            actual_end=a_end if status == "DONE" else None,
            distance_km=round(dist, 1), status=status))
    return missions


# --- Positions GPS (trajectoires simulées) ---------------------------------
def simulate_trace(m, rng, nrng, truth):
    """Trajectoire d'une mission DONE : un point toutes les 30 s (±2 s) sur la droite
    origine-destination. Met à jour m['actual_end'] avec la fin réelle de la trace."""
    o, d, plate = m["origin"], m["destination"], m["plate"]
    total = max(haversine(o.lat, o.lon, d.lat, d.lon), 1.0)
    brg = bearing(o.lat, o.lon, d.lat, d.lon)
    v_avg = clip(nrng.normal(55, 8), 35, 80)
    t, fuel = m["actual_start"], rng.uniform(55, 100)
    stop_km = stop_len = None
    theft = False
    if total > 30 and rng.random() < STOP_PROB:
        stop_km = rng.uniform(0.25, 0.75) * total
        stop_len = rng.randint(32, 60)  # minutes
        theft = rng.random() < THEFT_SHARE
        if theft:
            fuel = rng.uniform(88, 100)

    pts = []

    def add(lat, lon, speed, engine):
        ts = t + dt.timedelta(seconds=rng.randint(-2, 2))
        pts.append((plate, ts, round(lat, 6), round(lon, 6), round(speed, 1),
                    int(brg + rng.uniform(-4, 4)) % 360, round(fuel, 1), engine))

    add(o.lat, o.lon, 0.0, "IDLE")
    done_km, stopped = 0.0, False
    while done_km < total:
        t += dt.timedelta(seconds=30)
        s = clip(nrng.normal(v_avg, 12), 15, 105)
        done_km = min(total, done_km + s * 30 / 3600)
        fuel = max(3.0, fuel - s * 30 / 3600 * FUEL_PCT_PER_KM * rng.uniform(0.8, 1.2))
        f = done_km / total
        if done_km >= total:
            add(d.lat, d.lon, 0.0, "IDLE")
            break
        lat = o.lat + (d.lat - o.lat) * f + nrng.normal(0, 0.0002)
        lon = o.lon + (d.lon - o.lon) * f + nrng.normal(0, 0.0002)
        add(lat, lon, s, "ON")
        if stop_km is not None and not stopped and done_km >= stop_km:
            stopped = True
            slat, slon = lat, lon
            ramp, theft_at = 16, (rng.randint(4, stop_len - 12) * 2 if theft else None)
            fuel_before, drop = fuel, rng.uniform(25, 40)
            first_ts = None
            for k in range(stop_len * 2):
                t += dt.timedelta(seconds=30)
                if theft and k >= theft_at:
                    fuel = max(2.0, fuel_before - drop * min(1.0, (k - theft_at + 1) / ramp))
                    if first_ts is None:
                        first_ts = t
                add(slat + rng.uniform(-1e-5, 1e-5), slon + rng.uniform(-1e-5, 1e-5), 0.0, "OFF")
            truth["long_stops"].append(dict(mission_id=str(m["mission_id"]), plate=plate,
                                            start=(t - dt.timedelta(minutes=stop_len)).isoformat(),
                                            minutes=stop_len, fuel_theft=theft))
            if theft:
                truth["fuel_theft"].append(dict(plate=plate, mission_id=str(m["mission_id"]),
                                                start=first_ts.isoformat(), drop_pct=round(drop, 1)))

    # Sauts GPS isolés (un point déplacé de 5 à 13 km)
    for i in range(3, len(pts) - 3):
        if rng.random() < JUMP_PROB:
            p = list(pts[i])
            p[2] = round(p[2] + rng.choice([-1, 1]) * rng.uniform(0.05, 0.12), 6)
            pts[i] = tuple(p)
            truth["gps_jumps"].append(dict(plate=plate, ts=p[1].isoformat()))
    m["actual_end"] = pts[-1][1]
    return pts


# --- Événements de conduite ---------------------------------------------------
def gen_events(missions, drivers, target, rng, nrng):
    prop = {d["driver_id"]: (d["ps"], d["pb"]) for d in drivers}
    done = [m for m in missions if m["status"] == "DONE"]
    events = []

    def add(m, typ):
        frac = rng.random()
        ts = (m["actual_start"] + (m["actual_end"] - m["actual_start"]) * frac).replace(microsecond=0)
        o, d = m["origin"], m["destination"]
        if typ == "SPEEDING":
            value = round(clip(nrng.normal(112, 10), 91, 150), 1)       # km/h
        elif typ == "HARSH_BRAKING":
            value = round(rng.uniform(3.5, 8.0), 2)                     # m/s²
        elif typ == "LONG_IDLE":
            value = round(rng.uniform(10, 60), 1)                       # minutes
        else:
            value = round(rng.uniform(0.5, 15.0), 2)                    # km hors zone
        events.append(dict(driver_id=m["driver_id"], day=ts.date(), ts=ts,
                           event_id=uuid.UUID(int=rng.getrandbits(128), version=4),
                           plate=m["plate"], event_type=typ, value=value,
                           position=GeoPoint(round(o.lat + (d.lat - o.lat) * frac, 6),
                                             round(o.lon + (d.lon - o.lon) * frac, 6))))

    for m in done:
        s, b = prop[m["driver_id"]]
        for typ, lam in (("SPEEDING", 0.60 * s), ("HARSH_BRAKING", 0.45 * b),
                         ("LONG_IDLE", 0.25), ("GEOFENCE_EXIT", 0.15)):
            for _ in range(int(nrng.poisson(lam))):
                add(m, typ)
    while len(events) < target and done:
        add(rng.choice(done), rng.choices(EVENT_TYPES, [0.40, 0.30, 0.18, 0.12])[0])
    return events


# --- Maintenances -------------------------------------------------------------
def gen_maintenances(vehicles, today, rng):
    out = []
    for v in vehicles:
        n = rng.choices([1, 2, 3], [0.35, 0.40, 0.25])[0]
        d = today - dt.timedelta(days=rng.randint(60, 540))
        odo = rng.randint(20000, 300000)
        for _ in range(n):
            if d > today:
                break
            mtype = rng.choice(list(MAINT_CATALOG))
            parts = [PartItem(p[0], p[1], p[2], Decimal(p[3])) for p in MAINT_CATALOG[mtype]]
            cost = sum(p.qty * p.unit_cost for p in parts) + Decimal(rng.randint(200, 1500))
            out.append(dict(plate=v["plate"], maint_date=d,
                            maint_id=uuid.UUID(int=rng.getrandbits(128), version=4),
                            odometer_km=odo, maint_type=mtype, parts=parts,
                            cost=cost.quantize(Decimal("0.01"))))
            v["last_maint"] = d
            d += dt.timedelta(days=rng.randint(80, 200))
            odo += rng.randint(5000, 25000)
    return out


# --- Dernières positions (état "live" de la flotte) --------------------------
def gen_last_positions(vehicles, missions, depot_places, now, rng):
    live = {m["plate"]: m for m in missions if m["status"] == "IN_PROGRESS"}
    out = []
    for v in vehicles:
        m = live.get(v["plate"])
        if m:
            span = max((m["planned_end"] - m["actual_start"]).total_seconds(), 1)
            frac = clip((now - m["actual_start"]).total_seconds() / span, 0.02, 0.98)
            o, d = m["origin"], m["destination"]
            lat, lon = o.lat + (d.lat - o.lat) * frac, o.lon + (d.lon - o.lon) * frac
            ts, speed, eng = now - dt.timedelta(seconds=rng.randint(0, 90)), rng.uniform(40, 85), "ON"
        else:
            o = depot_places[v["depot_id"]]
            lat, lon = o.lat + rng.uniform(-0.2, 0.2), o.lon + rng.uniform(-0.2, 0.2)
            if rng.random() < 0.88:   # récent
                ts = now - dt.timedelta(minutes=rng.uniform(0, 50))
            else:                     # silencieux depuis plus d'1 h (cible de A21)
                ts = now - dt.timedelta(minutes=rng.uniform(65, 72 * 60))
            speed, eng = 0.0, rng.choice(["OFF", "IDLE"])
        out.append(dict(plate=v["plate"], ts=ts.replace(microsecond=0), lat=round(lat, 6),
                        lon=round(lon, 6), speed=round(speed, 1), fuel=round(rng.uniform(30, 100), 1),
                        engine=eng, geohash=pgh.encode(lat, lon, precision=5)))
    return out


# --- Génération globale -------------------------------------------------------
def generate(args):
    now = dt.datetime.fromisoformat(args.now) if args.now else \
        dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)
    rng, nrng = random.Random(args.seed), np.random.default_rng(args.seed)
    Faker.seed(args.seed)
    fake = Faker("fr_FR")
    s = args.scale
    n_veh, n_drv = max(10, int(1500 * s)), max(10, int(2000 * s))
    n_mis, n_pos, n_evt = int(12000 * s), int(120000 * s), int(16000 * s)

    depot_places = {d[0]: make_place(d[1], d[2], d[3], d[4]) for d in DEPOTS}
    depot_city = {d[0]: d[2] for d in DEPOTS}
    speed_factor = {d[0]: rng.uniform(0.88, 1.08) for d in DEPOTS}  # certains dépôts plus lents

    print(f"Date de référence : {now}   graine : {args.seed}   échelle : {s}")
    vehicles = gen_vehicles(n_veh, rng)
    drivers = gen_drivers(n_drv, rng, nrng, fake)
    print("Génération des missions...")
    missions = gen_missions(n_mis, max(5, int(60 * s)), vehicles, drivers, now, rng, nrng, fake,
                            depot_places, depot_city, speed_factor)

    print("Simulation des trajectoires GPS...")
    truth = dict(long_stops=[], fuel_theft=[], gps_jumps=[])
    tracked = set(rng.sample([v["plate"] for v in vehicles], max(1, int(len(vehicles) * 0.27))))
    limit = now - dt.timedelta(hours=4)
    cands = sorted((m for m in missions if m["status"] == "DONE" and m["plate"] in tracked
                    and m["actual_end"] <= limit), key=lambda m: m["actual_start"], reverse=True)
    positions, n_traced = [], 0
    with tqdm(total=n_pos, desc="positions", unit="pt") as bar:
        for m in cands:
            if len(positions) >= n_pos:
                break
            pts = simulate_trace(m, rng, nrng, truth)
            positions.extend(pts)
            n_traced += 1
            bar.update(len(pts))

    events = gen_events(missions, drivers, n_evt, rng, nrng)
    maints = gen_maintenances(vehicles, now.date(), rng)
    lastpos = gen_last_positions(vehicles, missions, depot_places, now, rng)

    # --- Agrégats pour les tables de compteurs ---
    vtype = {v["plate"]: v["vehicle_type"] for v in vehicles}
    vmodel = {v["plate"]: v["model"] for v in vehicles}
    agg = dict(by_day=defaultdict(lambda: [0, 0]), by_depot=defaultdict(lambda: [0, 0, 0]),
               ev_driver=defaultdict(int), ev_day=defaultdict(int),
               speed=defaultdict(lambda: [0, 0]), fuel=defaultdict(lambda: [0, 0]))
    for m in missions:
        if m["status"] != "DONE":
            continue
        day = m["actual_start"].date()
        agg["by_day"][day][0] += 1
        agg["by_day"][day][1] += int(m["distance_km"] * 1000)
        dep = agg["by_depot"][m["depot_id"]]
        dep[0] += 1
        dep[1] += 1 if m["actual_end"] > m["planned_end"] else 0
        dep[2] += int((m["actual_end"] - m["actual_start"]).total_seconds())
    for e in events:
        agg["ev_driver"][(e["driver_id"], e["event_type"])] += 1
        agg["ev_day"][(e["day"], e["event_type"])] += 1
    for p in positions:
        a = agg["speed"][vtype[p[0]]]
        a[0] += int(round(p[4] * 10)); a[1] += 1
        b = agg["fuel"][vmodel[p[0]]]
        b[0] += int(round(p[6] * 10)); b[1] += 1

    data = dict(now=now, depots=DEPOTS, depot_places=depot_places, vehicles=vehicles, drivers=drivers,
                missions=missions, positions=positions, events=events, maints=maints,
                lastpos=lastpos, agg=agg, truth=truth)
    summary(data, n_traced)
    return data


def summary(d, n_traced):
    ms = d["missions"]
    st = defaultdict(int)
    for m in ms:
        st[m["status"]] += 1
    done = [m for m in ms if m["status"] == "DONE"]
    late = sum(1 for m in done if m["actual_end"] > m["planned_end"])
    total = (len(d["vehicles"]) + len(d["drivers"]) + len(ms) + len(d["positions"])
             + len(d["events"]) + len(d["maints"]))
    print("\n=== Résumé ===")
    print(f"véhicules {len(d['vehicles'])} | chauffeurs {len(d['drivers'])} | missions {len(ms)} "
          f"| positions {len(d['positions'])} ({n_traced} trajets tracés) | événements {len(d['events'])} "
          f"| maintenances {len(d['maints'])}")
    print(f"total d'éléments : {total}  (exigence : >= 50 000)")
    print(f"statuts missions : {dict(st)}   retard global (DONE) : {late / max(1, len(done)):.1%}")
    t = d["truth"]
    print(f"anomalies injectées : {len(t['long_stops'])} arrêts > 30 min "
          f"({len(t['fuel_theft'])} avec baisse de carburant), {len(t['gps_jumps'])} sauts GPS")
    stale = sum(1 for p in d["lastpos"] if (d["now"] - p["ts"]).total_seconds() > 3600)
    print(f"véhicules sans position depuis > 1 h (cible A21) : {stale}")


# --- Chargement ------------------------------------------------------------
TABLES = ["depots", "vehicles", "vehicles_by_depot", "drivers", "positions_by_vehicle_day",
          "last_position_by_vehicle", "vehicles_by_geohash", "missions", "missions_by_driver_week",
          "active_missions_by_depot", "events_by_driver_day", "maintenances_by_vehicle"]
COUNTER_TABLES = ["mission_stats_by_day", "mission_stats_by_depot", "event_counts_by_driver",
                  "event_counts_by_day", "speed_agg_by_type", "fuel_agg_by_model"]


def monday(day):
    return day - dt.timedelta(days=day.weekday())


def load(args, d):
    from cassandra.concurrent import execute_concurrent_with_args
    from common.db import connect

    cluster, session = connect(args.cl, single_host=args.single_host)
    try:
        if args.truncate:
            for t in TABLES:
                session.execute(f"TRUNCATE {t}", timeout=120)
        for t in COUNTER_TABLES:  # tables dérivées : toujours remises à zéro
            session.execute(f"TRUNCATE {t}", timeout=120)

        def bulk(cql, rows, desc, chunk=5000, conc=64):
            stmt = session.prepare(cql)
            for i in tqdm(range(0, len(rows), chunk), desc=desc, unit=f"x{chunk}"):
                execute_concurrent_with_args(session, stmt, rows[i:i + chunk],
                                             concurrency=conc, raise_on_first_error=True)

        def cbulk(cql, rows, desc):
            # Compteurs : lecture avant écriture sur les réplicas, donc faible concurrence
            bulk(cql, rows, desc, chunk=1000, conc=8)

        bulk("INSERT INTO depots (depot_id,name,city,location) VALUES (?,?,?,?)",
             [(x[0], x[1], x[2], GeoPoint(x[3], x[4])) for x in d["depots"]], "depots")
        bulk("INSERT INTO vehicles (plate,vehicle_type,brand,model,year,capacity_kg,depot_id,equipment,"
             "features,last_maintenance_date) VALUES (?,?,?,?,?,?,?,?,?,?)",
             [(v["plate"], v["vehicle_type"], v["brand"], v["model"], v["year"], v["capacity_kg"],
               v["depot_id"], v["equipment"], v["features"], v["last_maint"]) for v in d["vehicles"]],
             "vehicles")
        bulk("INSERT INTO vehicles_by_depot (depot_id,plate,vehicle_type,model) VALUES (?,?,?,?)",
             [(v["depot_id"], v["plate"], v["vehicle_type"], v["model"]) for v in d["vehicles"]],
             "vehicles_by_depot")
        bulk("INSERT INTO drivers (driver_id,name,license_categories,hire_date,depot_id) VALUES (?,?,?,?,?)",
             [(x["driver_id"], x["name"], x["license_categories"], x["hire_date"], x["depot_id"])
              for x in d["drivers"]], "drivers")
        bulk("INSERT INTO positions_by_vehicle_day (plate,day,ts,lat,lon,speed_kmh,heading,fuel_pct,"
             "engine_state) VALUES (?,?,?,?,?,?,?,?,?)",
             [(p[0], p[1].date(), p[1], p[2], p[3], p[4], p[5], p[6], p[7]) for p in d["positions"]],
             "positions", chunk=10000)
        bulk("INSERT INTO last_position_by_vehicle (plate,ts,lat,lon,speed_kmh,fuel_pct,engine_state,"
             "geohash) VALUES (?,?,?,?,?,?,?,?)",
             [(x["plate"], x["ts"], x["lat"], x["lon"], x["speed"], x["fuel"], x["engine"], x["geohash"])
              for x in d["lastpos"]], "last_position")
        # TTL de 24 h pour ce snapshot (la table a un TTL par défaut de 30 min, pensé pour le flux temps réel)
        bulk("INSERT INTO vehicles_by_geohash (geohash,plate,ts,lat,lon) VALUES (?,?,?,?,?) USING TTL 86400",
             [(x["geohash"], x["plate"], x["ts"], x["lat"], x["lon"]) for x in d["lastpos"]], "geohash")
        ms = d["missions"]
        bulk("INSERT INTO missions (mission_id,plate,driver_id,depot_id,origin,destination,planned_start,"
             "planned_end,actual_start,actual_end,distance_km,status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
             [(m["mission_id"], m["plate"], m["driver_id"], m["depot_id"], m["origin"], m["destination"],
               m["planned_start"], m["planned_end"], m["actual_start"], m["actual_end"],
               m["distance_km"], m["status"]) for m in ms], "missions")
        bulk("INSERT INTO missions_by_driver_week (driver_id,week_start,planned_start,mission_id,plate,"
             "status,distance_km) VALUES (?,?,?,?,?,?,?)",
             [(m["driver_id"], monday(m["planned_start"].date()), m["planned_start"], m["mission_id"],
               m["plate"], m["status"], m["distance_km"]) for m in ms], "missions_by_driver_week")
        bulk("INSERT INTO active_missions_by_depot (depot_id,mission_id,plate,driver_id,actual_start) "
             "VALUES (?,?,?,?,?)",
             [(m["depot_id"], m["mission_id"], m["plate"], m["driver_id"], m["actual_start"])
              for m in ms if m["status"] == "IN_PROGRESS"], "active_missions")
        bulk("INSERT INTO events_by_driver_day (driver_id,day,ts,event_id,plate,event_type,value,position) "
             "VALUES (?,?,?,?,?,?,?,?)",
             [(e["driver_id"], e["day"], e["ts"], e["event_id"], e["plate"], e["event_type"], e["value"],
               e["position"]) for e in d["events"]], "events")
        bulk("INSERT INTO maintenances_by_vehicle (plate,maint_date,maint_id,odometer_km,maint_type,parts,"
             "cost) VALUES (?,?,?,?,?,?,?)",
             [(x["plate"], x["maint_date"], x["maint_id"], x["odometer_km"], x["maint_type"], x["parts"],
               x["cost"]) for x in d["maints"]], "maintenances")

        a = d["agg"]
        cbulk("UPDATE mission_stats_by_day SET missions_count = missions_count + ?, "
             "distance_m = distance_m + ? WHERE day = ?",
             [(v[0], v[1], k) for k, v in a["by_day"].items()], "stats/jour")
        cbulk("UPDATE mission_stats_by_depot SET missions_done = missions_done + ?, "
             "missions_late = missions_late + ?, total_duration_s = total_duration_s + ? WHERE depot_id = ?",
             [(v[0], v[1], v[2], k) for k, v in a["by_depot"].items()], "stats/dépôt")
        cbulk("UPDATE event_counts_by_driver SET cnt = cnt + ? WHERE driver_id = ? AND event_type = ?",
             [(c, k[0], k[1]) for k, c in a["ev_driver"].items()], "events/chauffeur")
        cbulk("UPDATE event_counts_by_day SET cnt = cnt + ? WHERE day = ? AND event_type = ?",
             [(c, k[0], k[1]) for k, c in a["ev_day"].items()], "events/jour")
        cbulk("UPDATE speed_agg_by_type SET speed_sum_x10 = speed_sum_x10 + ?, samples = samples + ? "
             "WHERE vehicle_type = ?", [(v[0], v[1], k) for k, v in a["speed"].items()], "vitesse/type")
        cbulk("UPDATE fuel_agg_by_model SET fuel_sum_x10 = fuel_sum_x10 + ?, samples = samples + ? "
             "WHERE model = ?", [(v[0], v[1], k) for k, v in a["fuel"].items()], "carburant/modèle")
        print("\nChargement terminé.")
    finally:
        cluster.shutdown()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", type=float, default=1.0, help="1.0 = volumes du sujet; 0.1 = jeu de dev")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--now", help="date de référence ISO (UTC) pour des données identiques chez A et B")
    ap.add_argument("--cl", default=None, help="niveau de cohérence du chargement (ONE, QUORUM, ALL)")
    ap.add_argument("--truncate", action="store_true", help="vide les tables avant chargement")
    ap.add_argument("--single-host", action="store_true", default=None,
                    help="ne parler qu'aux hôtes de CASSANDRA_HOSTS")
    ap.add_argument("--dry-run", action="store_true", help="génère sans écrire dans Cassandra")
    args = ap.parse_args()

    data = generate(args)
    os.makedirs("data", exist_ok=True)
    with open("data/ground_truth.json", "w", encoding="utf-8") as f:
        json.dump(dict(reference_now=data["now"].isoformat(), **data["truth"]), f, indent=1)
    print("Vérité terrain des anomalies : data/ground_truth.json")
    if not args.dry_run:
        load(args, data)


if __name__ == "__main__":
    main()
