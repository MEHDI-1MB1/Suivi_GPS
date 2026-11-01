#!/usr/bin/env python3
"""Analytiques natives A16-A25 (rôle A) + banc de mesure.

Depuis la racine du dépôt :
  python -m queries.analytics --repeat 10
  python -m queries.analytics --cl QUORUM --repeat 10
  python -m queries.analytics --lat 33.57 --lon -7.59      # point central de A20

Principe : Cassandra n'a ni GROUP BY ni jointure. Chaque analytique est donc servie par
  - une table d'agrégats précalculés (compteurs) : A16, A18, A19, A22, A23, A25
  - un balayage d'une petite table + traitement client : A17, A21, A24
  - une recherche par cellules geohash + filtre Haversine : A20
"""
import argparse
import csv
import datetime as dt
import heapq
import math
import os
import statistics
import time

import pygeohash as pgh
from cassandra.concurrent import execute_concurrent_with_args

from common.db import connect, timed
from data.generate_and_load import haversine

GEOHASH_PRECISION = 5  # doit correspondre à la précision écrite dans vehicles_by_geohash


_NOW = None  # date de référence imposée par --now (sinon l'heure réelle)


def utcnow():
    return _NOW or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def as_date(d):
    return d.date() if hasattr(d, "date") and not isinstance(d, dt.datetime) else d


def geohash_cells(lat, lon, radius_km, precision=GEOHASH_PRECISION):
    """Cellules geohash à interroger pour couvrir un disque de rayon radius_km.
    La cellule centrale ne suffit pas : on ajoute k anneaux de voisines, avec
    k = ceil(rayon / plus petit côté de cellule). En précision 5 (~4,9 km x ~4,1 km
    au Maroc) et pour 5 km, k = 2 soit 25 cellules (la cellule + 8 voisines manque des véhicules)."""
    gh = pgh.encode(lat, lon, precision=precision)
    clat, clon, elat, elon = pgh.decode_exactly(gh)
    h = 2 * elat * 111.32
    w = 2 * elon * 111.32 * math.cos(math.radians(clat))
    k = math.ceil(radius_km / min(h, w))
    return {pgh.encode(clat + dy * 2 * elat, clon + dx * 2 * elon, precision=precision)
            for dy in range(-k, k + 1) for dx in range(-k, k + 1)}


class Analytics:
    def __init__(self, session):
        self.s = session
        p = session.prepare
        self.q16 = p("SELECT day, missions_count, distance_m FROM mission_stats_by_day WHERE day=?")
        self.q17 = p("SELECT driver_id, event_type, cnt FROM event_counts_by_driver")
        self.q17.fetch_size = 2000
        self.q_name = p("SELECT driver_id, name FROM drivers WHERE driver_id=?")
        self.q18 = p("SELECT depot_id, missions_done, missions_late, total_duration_s FROM mission_stats_by_depot")
        self.q19 = p("SELECT vehicle_type, speed_sum_x10, samples FROM speed_agg_by_type")
        self.q20 = p("SELECT plate, lat, lon FROM vehicles_by_geohash WHERE geohash=?")
        self.q21 = p("SELECT plate, ts FROM last_position_by_vehicle")
        self.q21.fetch_size = 2000
        self.q22 = p("SELECT event_type, cnt FROM event_counts_by_day WHERE day=?")
        self.q23 = p("SELECT model, fuel_sum_x10, samples FROM fuel_agg_by_model")
        self.q24 = p("SELECT plate, last_maintenance_date FROM vehicles")
        self.q24.fetch_size = 2000

    # A16 : missions et distance par jour sur 30 jours (30 lectures de partitions en parallèle)
    def a16_missions_per_day(self, days=30):
        today = utcnow().date()
        ds = [today - dt.timedelta(days=i) for i in range(days - 1, -1, -1)]
        res = execute_concurrent_with_args(self.s, self.q16, [(d,) for d in ds], concurrency=30)
        out = []
        for d, (ok, rs) in zip(ds, res):
            r = rs.one()
            out.append((d, (r.missions_count or 0) if r else 0, round((r.distance_m or 0) / 1000, 1) if r else 0.0))
        return out

    # A17 : top 10 des chauffeurs par excès de vitesse (balayage des compteurs, tri côté client)
    def a17_top_speeders(self, k=10):
        best = heapq.nlargest(k, ((r.cnt, r.driver_id) for r in self.s.execute(self.q17)
                                  if r.event_type == "SPEEDING"))
        out = []
        for cnt, did in best:
            row = self.s.execute(self.q_name, (did,)).one()
            out.append((did, row.name if row else "?", cnt))
        return out

    # A18 : taux de missions en retard par dépôt
    def a18_late_rate(self):
        rows = [(r.depot_id, r.missions_done, r.missions_late) for r in self.s.execute(self.q18)]
        return sorted(((d, done, late, round(100.0 * late / done, 1) if done else 0.0)
                       for d, done, late in rows), key=lambda x: -x[3])

    # A19 : vitesse moyenne par type de véhicule (tous points GPS, arrêts compris)
    def a19_avg_speed_by_type(self):
        return sorted(((r.vehicle_type, round(r.speed_sum_x10 / 10 / r.samples, 1), r.samples)
                       for r in self.s.execute(self.q19) if r.samples), key=lambda x: -x[1])

    # A20 : véhicules dans un rayon de 5 km (geohash + Haversine)
    def a20_within_radius(self, lat, lon, radius_km=5.0):
        cells = geohash_cells(lat, lon, radius_km)
        res = execute_concurrent_with_args(self.s, self.q20, [(c,) for c in cells], concurrency=32)
        found = []
        for ok, rs in res:
            for r in rs:
                d = haversine(lat, lon, r.lat, r.lon)
                if d <= radius_km:
                    found.append((round(d, 2), r.plate))
        return sorted(found)

    # A21 : véhicules sans position depuis 1 h (balayage des 1 500 dernières positions)
    def a21_silent_vehicles(self, hours=1.0):
        cutoff = utcnow() - dt.timedelta(hours=hours)
        return sorted(((r.plate, r.ts) for r in self.s.execute(self.q21) if r.ts < cutoff), key=lambda x: x[1])

    # A22 : événements par type et par jour (30 jours)
    def a22_events_per_day(self, days=30):
        today = utcnow().date()
        ds = [today - dt.timedelta(days=i) for i in range(days - 1, -1, -1)]
        res = execute_concurrent_with_args(self.s, self.q22, [(d,) for d in ds], concurrency=30)
        return {d: {r.event_type: r.cnt for r in rs} for d, (ok, rs) in zip(ds, res)}

    # A23 : niveau de carburant moyen par modèle
    def a23_avg_fuel_by_model(self):
        return sorted(((r.model, round(r.fuel_sum_x10 / 10 / r.samples, 1), r.samples)
                       for r in self.s.execute(self.q23) if r.samples), key=lambda x: x[1])

    # A24 : véhicules dont la dernière maintenance date de plus de 6 mois (ou jamais entretenus)
    def a24_overdue_maintenance(self, months_days=182):
        limit = utcnow().date() - dt.timedelta(days=months_days)
        out = []
        for r in self.s.execute(self.q24):
            last = as_date(r.last_maintenance_date) if r.last_maintenance_date else None
            if last is None or last < limit:
                out.append((r.plate, last))
        return sorted(out, key=lambda x: (x[1] is not None, x[1] or dt.date.min))

    # A25 : durée moyenne des missions par dépôt (minutes)
    def a25_avg_duration_by_depot(self):
        return sorted(((r.depot_id, round(r.total_duration_s / r.missions_done / 60, 1), r.missions_done)
                       for r in self.s.execute(self.q18) if r.missions_done), key=lambda x: -x[1])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cl", default=None, help="niveau de cohérence (ONE, QUORUM, ALL)")
    ap.add_argument("--repeat", type=int, default=10, help="exécutions par requête")
    ap.add_argument("--lat", type=float, help="latitude du centre de A20")
    ap.add_argument("--lon", type=float, help="longitude du centre de A20")
    ap.add_argument("--now", help="date de référence ISO utilisée au chargement (ex. 2026-10-31T12:00:00)")
    args = ap.parse_args()
    global _NOW
    if args.now:
        _NOW = dt.datetime.fromisoformat(args.now)

    cluster, s = connect(args.cl)
    a = Analytics(s)
    cl_name = (args.cl or os.getenv("CASSANDRA_CL", "ONE")).upper()

    lat, lon = args.lat, args.lon
    if lat is None or lon is None:  # par défaut : position d'un véhicule connu, pour avoir au moins 1 résultat
        row = s.execute("SELECT lat, lon FROM vehicles_by_geohash LIMIT 1").one()
        if row is None:
            raise SystemExit("Aucune donnée : lancez d'abord  python -m data.generate_and_load --truncate")
        lat, lon = row.lat, row.lon

    tests = [
        ("A16", a.a16_missions_per_day), ("A17", a.a17_top_speeders), ("A18", a.a18_late_rate),
        ("A19", a.a19_avg_speed_by_type), ("A20", lambda: a.a20_within_radius(lat, lon)),
        ("A21", a.a21_silent_vehicles), ("A22", a.a22_events_per_day), ("A23", a.a23_avg_fuel_by_model),
        ("A24", a.a24_overdue_maintenance), ("A25", a.a25_avg_duration_by_depot),
    ]
    results, samples = [], {}
    for label, fn in tests:
        for i in range(args.repeat):
            with timed(label, results):
                out = fn()
            samples[label] = out

    # --------------------------------------------------------- aperçu des résultats
    print(f"\n=== Aperçu des résultats (CL={cl_name}) ===")
    m = samples["A16"]
    print(f"A16  30 jours : {sum(x[1] for x in m)} missions, {sum(x[2] for x in m):.0f} km "
          f"(dernier jour : {m[-1][0]} -> {m[-1][1]} missions)")
    print("A17  top 3 :", samples["A17"][:3])
    print("A18  taux de retard :", [(d, f"{r}%") for d, _, _, r in samples["A18"][:4]], "...")
    print("A19  vitesse moyenne :", samples["A19"])
    print(f"A20  {len(samples['A20'])} véhicule(s) à <= 5 km de ({lat:.4f}, {lon:.4f}) :", samples["A20"][:3])
    print(f"A21  {len(samples['A21'])} véhicule(s) sans position depuis 1 h :", samples["A21"][:2])
    tot = {}
    for per in samples["A22"].values():
        for t, c in per.items():
            tot[t] = tot.get(t, 0) + c
    print("A22  événements sur 30 jours :", tot)
    print("A23  carburant moyen (3 plus bas) :", samples["A23"][:3])
    print(f"A24  {len(samples['A24'])} véhicule(s) sans maintenance depuis 6 mois :", samples["A24"][:2])
    print("A25  durée moyenne (min) :", samples["A25"][:4], "...")

    by = {}
    for r in results:
        by.setdefault(r["label"], []).append(r["ms"])
    print(f"\n=== Temps (CL={cl_name}, {args.repeat} exécutions) ===")
    print(f"{'requête':<10}{'médiane ms':>12}{'min ms':>10}{'max ms':>10}")
    for label, v in by.items():
        print(f"{label:<10}{statistics.median(v):>12.2f}{min(v):>10.2f}{max(v):>10.2f}")

    out_dir = os.path.join("benchmarks", "results")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"analytics_{cl_name}_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["query", "consistency", "run", "ms"])
        c = {}
        for r in results:
            c[r["label"]] = c.get(r["label"], 0) + 1
            w.writerow([r["label"], cl_name, c[r["label"]], f"{r['ms']:.3f}"])
    print(f"Mesures enregistrées dans {path}")
    cluster.shutdown()


if __name__ == "__main__":
    main()
