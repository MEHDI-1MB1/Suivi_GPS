#!/usr/bin/env python3
"""Requêtes transactionnelles T1-T15 (rôle A) + banc de mesure.

Depuis la racine du dépôt :
  python -m queries.transactional                       # CL par défaut (CASSANDRA_CL, sinon ONE)
  python -m queries.transactional --cl QUORUM --repeat 30

Chaque requête est exécutée --repeat fois ; le temps (ms) de chaque exécution est
écrit dans benchmarks/results/transactional_<CL>_<horodatage>.csv.

Attention : les écritures (T1, T6-T9, T11, T12) modifient réellement les tables.
Les positions de test (T2, T13, T14) utilisent des plaques fictives "BULK-xxxx" avec TTL.
Rechargez avec  python -m data.generate_and_load --truncate  avant les mesures finales.
"""
import argparse
import csv
import datetime as dt
import os
import random
import statistics
import time
import uuid
from decimal import Decimal

import pygeohash as pgh
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import UNSET_VALUE, BatchStatement, BatchType

from common.db import connect, timed
from data.generate_and_load import GeoPoint, PartItem, Place, monday


def utcnow():
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def as_date(d):
    """Les colonnes `date` reviennent en cassandra.util.Date ; on veut un datetime.date."""
    return d.date() if hasattr(d, "date") and not isinstance(d, dt.datetime) else d


class Transactional:
    def __init__(self, session):
        self.s = session
        p = session.prepare
        # --- positions
        pos_cols = "(plate,day,ts,lat,lon,speed_kmh,heading,fuel_pct,engine_state) VALUES (?,?,?,?,?,?,?,?,?)"
        self.ins_pos = p("INSERT INTO positions_by_vehicle_day " + pos_cols)
        self.ins_pos_ttl = p("INSERT INTO positions_by_vehicle_day " + pos_cols + " USING TTL ?")
        self.upd_last = p("INSERT INTO last_position_by_vehicle (plate,ts,lat,lon,speed_kmh,fuel_pct,"
                          "engine_state,geohash) VALUES (?,?,?,?,?,?,?,?)")
        self.sel_last = p("SELECT * FROM last_position_by_vehicle WHERE plate=?")
        self.ins_geo = p("INSERT INTO vehicles_by_geohash (geohash,plate,ts,lat,lon) VALUES (?,?,?,?,?)")
        self.del_geo = p("DELETE FROM vehicles_by_geohash WHERE geohash=? AND plate=?")
        self.sel_trace = p("SELECT ts,lat,lon,speed_kmh,heading,fuel_pct,engine_state "
                           "FROM positions_by_vehicle_day WHERE plate=? AND day=?")
        self.del_range = p("DELETE FROM positions_by_vehicle_day WHERE plate=? AND day=? AND ts>=? AND ts<=?")
        self.cnt_pos_day = p("SELECT COUNT(*) FROM positions_by_vehicle_day WHERE plate=? AND day=?")
        self.sel_ttl = p("SELECT TTL(lat) AS ttl FROM positions_by_vehicle_day WHERE plate=? AND day=? LIMIT 1")
        # --- véhicules
        self.sel_vehicle = p("SELECT * FROM vehicles WHERE plate=?")
        self.add_equip = p("UPDATE vehicles SET equipment = equipment + ? WHERE plate=?")
        self.upd_last_maint = p("UPDATE vehicles SET last_maintenance_date=? WHERE plate=?")
        # --- missions
        self.ins_mission = p("INSERT INTO missions (mission_id,plate,driver_id,depot_id,origin,destination,"
                             "planned_start,planned_end,distance_km,status) "
                             "VALUES (?,?,?,?,?,?,?,?,?,'PLANNED')")
        self.sel_mission = p("SELECT * FROM missions WHERE mission_id=?")
        self.upd_driver = p("UPDATE missions SET driver_id=? WHERE mission_id=?")
        self.upd_start = p("UPDATE missions SET actual_start=?, status='IN_PROGRESS' WHERE mission_id=?")
        self.upd_end = p("UPDATE missions SET actual_end=?, status='DONE' WHERE mission_id=?")
        self.ins_dw = p("INSERT INTO missions_by_driver_week (driver_id,week_start,planned_start,mission_id,"
                        "plate,status,distance_km) VALUES (?,?,?,?,?,?,?)")
        self.del_dw = p("DELETE FROM missions_by_driver_week WHERE driver_id=? AND week_start=? "
                        "AND planned_start=? AND mission_id=?")
        self.upd_dw_status = p("UPDATE missions_by_driver_week SET status=? WHERE driver_id=? AND week_start=? "
                               "AND planned_start=? AND mission_id=?")
        self.sel_dw = p("SELECT * FROM missions_by_driver_week WHERE driver_id=? AND week_start=?")
        self.ins_active = p("INSERT INTO active_missions_by_depot (depot_id,mission_id,plate,driver_id,"
                            "actual_start) VALUES (?,?,?,?,?)")
        self.upd_active_driver = p("UPDATE active_missions_by_depot SET driver_id=? WHERE depot_id=? AND mission_id=?")
        self.del_active = p("DELETE FROM active_missions_by_depot WHERE depot_id=? AND mission_id=?")
        self.sel_active = p("SELECT * FROM active_missions_by_depot WHERE depot_id=?")
        # --- événements et maintenances
        self.ins_event = p("INSERT INTO events_by_driver_day (driver_id,day,ts,event_id,plate,event_type,value,"
                           "position) VALUES (?,?,?,?,?,?,?,?)")
        self.ins_maint = p("INSERT INTO maintenances_by_vehicle (plate,maint_date,maint_id,odometer_km,"
                           "maint_type,parts,cost) VALUES (?,?,?,?,?,?,?)")
        # --- compteurs
        self.c_ev_driver = p("UPDATE event_counts_by_driver SET cnt = cnt + ? WHERE driver_id=? AND event_type=?")
        self.c_ev_day = p("UPDATE event_counts_by_day SET cnt = cnt + ? WHERE day=? AND event_type=?")
        self.c_by_day = p("UPDATE mission_stats_by_day SET missions_count = missions_count + ?, "
                          "distance_m = distance_m + ? WHERE day=?")
        self.c_by_depot = p("UPDATE mission_stats_by_depot SET missions_done = missions_done + ?, "
                            "missions_late = missions_late + ?, total_duration_s = total_duration_s + ? "
                            "WHERE depot_id=?")

    # ------------------------------------------------------------------ T1
    def t1_record_position(self, plate, ts, lat, lon, speed, heading, fuel, engine):
        """Position + dernière position + cellule geohash dans un batch logué (atomique).
        Une lecture préalable retire le véhicule de son ancienne cellule geohash."""
        gh = pgh.encode(lat, lon, precision=5)
        old = self.s.execute(self.sel_last, (plate,)).one()
        b = BatchStatement(batch_type=BatchType.LOGGED)
        b.add(self.ins_pos, (plate, ts.date(), ts, lat, lon, speed, heading, fuel, engine))
        b.add(self.upd_last, (plate, ts, lat, lon, speed, fuel, engine, gh))
        b.add(self.ins_geo, (gh, plate, ts, lat, lon))
        if old and old.geohash and old.geohash != gh:
            b.add(self.del_geo, (old.geohash, plate))
        self.s.execute(b)

    # ------------------------------------------------------------------ T2
    def t2_bulk_positions(self, n=10000, base=None, n_plates=100, ttl=3600, concurrency=128):
        """Charge n positions en lot (requêtes asynchrones concurrentes, sans batch :
        les lignes appartiennent à des partitions différentes). TTL court = auto-nettoyage."""
        base = base or utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - dt.timedelta(days=3)
        rows = []
        for i in range(n):
            plate = f"BULK-{i % n_plates:04d}"
            ts = base + dt.timedelta(seconds=30 * (i // n_plates))
            rows.append((plate, ts.date(), ts, 33.0 + random.random(), -8.0 + random.random(),
                         round(random.uniform(0, 100), 1), random.randint(0, 359),
                         round(random.uniform(20, 100), 1), "ON", ttl))
        execute_concurrent_with_args(self.s, self.ins_pos_ttl, rows, concurrency=concurrency,
                                     raise_on_first_error=True)
        return base

    # ------------------------------------------------------------------ T3, T4, T5
    def t3_vehicle_card(self, plate):
        return self.s.execute(self.sel_vehicle, (plate,)).one()

    def t4_last_position(self, plate):
        return self.s.execute(self.sel_last, (plate,)).one()

    def t5_day_trace(self, plate, day):
        return list(self.s.execute(self.sel_trace, (plate, day)))

    # ------------------------------------------------------------------ T6
    def t6_create_mission(self, plate, depot_id, origin, destination, planned_start, planned_end,
                          distance_km, driver_id=None):
        mid = uuid.uuid4()
        self.s.execute(self.ins_mission, (mid, plate, driver_id if driver_id else UNSET_VALUE, depot_id,
                                          origin, destination, planned_start, planned_end, distance_km))
        if driver_id:
            self.s.execute(self.ins_dw, (driver_id, monday(planned_start.date()), planned_start, mid,
                                         plate, "PLANNED", distance_km))
        return mid

    # ------------------------------------------------------------------ T7
    def t7_start_mission(self, mission_id, when=None):
        when = when or utcnow()
        m = self.s.execute(self.sel_mission, (mission_id,)).one()
        if m is None:
            raise ValueError("mission inconnue")
        b = BatchStatement(batch_type=BatchType.LOGGED)
        b.add(self.upd_start, (when, mission_id))
        b.add(self.ins_active, (m.depot_id, mission_id, m.plate, m.driver_id, when))
        if m.driver_id:
            b.add(self.upd_dw_status, ("IN_PROGRESS", m.driver_id, monday(m.planned_start.date()),
                                       m.planned_start, mission_id))
        self.s.execute(b)

    def t7_end_mission(self, mission_id, when=None):
        when = when or utcnow()
        m = self.s.execute(self.sel_mission, (mission_id,)).one()
        if m is None or m.actual_start is None:
            raise ValueError("mission inconnue ou non démarrée")
        b = BatchStatement(batch_type=BatchType.LOGGED)
        b.add(self.upd_end, (when, mission_id))
        b.add(self.del_active, (m.depot_id, mission_id))
        if m.driver_id:
            b.add(self.upd_dw_status, ("DONE", m.driver_id, monday(m.planned_start.date()),
                                       m.planned_start, mission_id))
        self.s.execute(b)
        # Les compteurs ne se mélangent pas aux autres écritures : batch COUNTER séparé.
        late = 1 if when > m.planned_end else 0
        cb = BatchStatement(batch_type=BatchType.COUNTER)
        cb.add(self.c_by_day, (1, int((m.distance_km or 0) * 1000), m.actual_start.date()))
        cb.add(self.c_by_depot, (1, late, int((when - m.actual_start).total_seconds()), m.depot_id))
        self.s.execute(cb)

    # ------------------------------------------------------------------ T8
    def t8_assign_driver(self, mission_id, driver_id):
        m = self.s.execute(self.sel_mission, (mission_id,)).one()
        if m is None:
            raise ValueError("mission inconnue")
        wk = monday(m.planned_start.date())
        b = BatchStatement(batch_type=BatchType.LOGGED)
        b.add(self.upd_driver, (driver_id, mission_id))
        if m.driver_id and m.driver_id != driver_id:  # ré-affectation : retirer l'ancienne ligne
            b.add(self.del_dw, (m.driver_id, wk, m.planned_start, mission_id))
        b.add(self.ins_dw, (driver_id, wk, m.planned_start, mission_id, m.plate, m.status, m.distance_km))
        if m.status == "IN_PROGRESS":
            b.add(self.upd_active_driver, (driver_id, m.depot_id, mission_id))
        self.s.execute(b)

    # ------------------------------------------------------------------ T9
    def t9_record_event(self, driver_id, plate, event_type, ts, value, lat, lon):
        self.s.execute(self.ins_event, (driver_id, ts.date(), ts, uuid.uuid4(), plate, event_type, value,
                                        GeoPoint(lat, lon)))
        cb = BatchStatement(batch_type=BatchType.COUNTER)
        cb.add(self.c_ev_driver, (1, driver_id, event_type))
        cb.add(self.c_ev_day, (1, ts.date(), event_type))
        self.s.execute(cb)

    # ------------------------------------------------------------------ T10
    def t10_driver_week(self, driver_id, any_day_of_week):
        return list(self.s.execute(self.sel_dw, (driver_id, monday(any_day_of_week))))

    # ------------------------------------------------------------------ T11
    def t11_record_maintenance(self, plate, maint_date, odometer_km, maint_type, parts, cost):
        cur = self.s.execute(self.sel_vehicle, (plate,)).one()
        cur_last = as_date(cur.last_maintenance_date) if cur and cur.last_maintenance_date else None
        b = BatchStatement(batch_type=BatchType.LOGGED)
        b.add(self.ins_maint, (plate, maint_date, uuid.uuid4(), odometer_km, maint_type, parts, cost))
        if cur_last is None or maint_date >= cur_last:  # ne jamais reculer la date de dernière maintenance
            b.add(self.upd_last_maint, (maint_date, plate))
        self.s.execute(b)

    # ------------------------------------------------------------------ T12
    def t12_add_equipment(self, plate, item):
        self.s.execute(self.add_equip, ({item}, plate))

    # ------------------------------------------------------------------ T13
    def t13_ttl_demo(self, sample_plate, sample_day, seconds=5):
        """Montre (1) le TTL par défaut de 60 jours sur une ligne chargée, (2) l'expiration
        automatique d'une ligne écrite avec un TTL de quelques secondes."""
        row = self.s.execute(self.sel_ttl, (sample_plate, sample_day)).one()
        default_ttl = row.ttl if row else None
        plate, ts = "BULK-TTL", utcnow()
        self.s.execute(self.ins_pos_ttl, (plate, ts.date(), ts, 33.5, -7.6, 50.0, 90, 60.0, "ON", seconds))
        ttl_now = self.s.execute(self.sel_ttl, (plate, ts.date())).one().ttl
        before = self.s.execute(self.cnt_pos_day, (plate, ts.date())).one()[0]
        time.sleep(seconds + 1)
        after = self.s.execute(self.cnt_pos_day, (plate, ts.date())).one()[0]
        return dict(default_ttl_s=default_ttl, default_ttl_days=round(default_ttl / 86400, 1) if default_ttl else None,
                    ttl_restant_s=ttl_now, lignes_avant=before, lignes_apres=after)

    # ------------------------------------------------------------------ T14
    def t14_delete_range(self, plate, start, end):
        """Supprime les positions d'un boîtier entre start et end : une suppression par plage
        (tombstone de plage) par jour concerné, envoyées en parallèle."""
        params, d = [], start.date()
        while d <= end.date():
            lo = max(start, dt.datetime.combine(d, dt.time.min))
            hi = min(end, dt.datetime.combine(d, dt.time.max))
            params.append((plate, d, lo, hi))
            d += dt.timedelta(days=1)
        execute_concurrent_with_args(self.s, self.del_range, params, concurrency=16,
                                     raise_on_first_error=True)
        return len(params)

    def count_positions(self, plate, start, end):
        total, d = 0, start.date()
        while d <= end.date():
            total += self.s.execute(self.cnt_pos_day, (plate, d)).one()[0]
            d += dt.timedelta(days=1)
        return total

    # ------------------------------------------------------------------ T15
    def t15_active_missions(self, depot_id):
        return list(self.s.execute(self.sel_active, (depot_id,)))


# ===================================================================== banc de mesure
def sample_refs(s):
    refs = dict(
        plates=[r.plate for r in s.execute("SELECT plate FROM vehicles LIMIT 60")],
        traces=[(r.plate, r.day) for r in s.execute("SELECT DISTINCT plate, day FROM positions_by_vehicle_day LIMIT 40")],
        drivers=[r.driver_id for r in s.execute("SELECT driver_id FROM drivers LIMIT 60")],
        weeks=[(r.driver_id, as_date(r.week_start)) for r in
               s.execute("SELECT DISTINCT driver_id, week_start FROM missions_by_driver_week LIMIT 40")],
        depots=[r.depot_id for r in s.execute("SELECT depot_id FROM depots")],
        veh_depot={r.plate: r.depot_id for r in s.execute("SELECT plate, depot_id FROM vehicles LIMIT 60")},
    )
    if not (refs["plates"] and refs["traces"] and refs["drivers"] and refs["weeks"]):
        raise SystemExit("Tables vides : lancez d'abord  python -m data.generate_and_load --truncate")
    return refs


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cl", default=None, help="niveau de cohérence (ONE, QUORUM, ALL)")
    ap.add_argument("--repeat", type=int, default=20, help="exécutions par requête")
    args = ap.parse_args()

    cluster, s = connect(args.cl)
    q = Transactional(s)
    refs = sample_refs(s)
    cl_name = (args.cl or os.getenv("CASSANDRA_CL", "ONE")).upper()
    results, n = [], args.repeat
    pick = lambda lst, i: lst[i % len(lst)]

    def bench(label, fn):
        for i in range(n):
            with timed(label, results):
                fn(i)

    t_place = lambda: Place("Dépôt test", "", 33.5731, -7.5898, pgh.encode(33.5731, -7.5898, precision=7))

    # T1 : une position pour un véhicule différent à chaque exécution
    bench("T1", lambda i: q.t1_record_position(pick(refs["plates"], i), utcnow(), 33.5 + i * 1e-3,
                                               -7.6 - i * 1e-3, 60.0, 90, 70.0, "ON"))
    # T2 : lot de 10 000 positions (1 seule mesure par répétition, plus longue)
    base = None
    for _ in range(max(1, min(n, 5))):
        with timed("T2", results):
            base = q.t2_bulk_positions(10000, base=base)
    bench("T3", lambda i: q.t3_vehicle_card(pick(refs["plates"], i)))
    bench("T4", lambda i: q.t4_last_position(pick(refs["plates"], i)))
    bench("T5", lambda i: q.t5_day_trace(*pick(refs["traces"], i)))

    mids = []
    now = utcnow()
    bench("T6", lambda i: mids.append(q.t6_create_mission(
        pick(refs["plates"], i), refs["veh_depot"][pick(refs["plates"], i)], t_place(), t_place(),
        now + dt.timedelta(hours=1), now + dt.timedelta(hours=3), 120.0)))
    bench("T8", lambda i: q.t8_assign_driver(mids[i], pick(refs["drivers"], i)))
    bench("T7_start", lambda i: q.t7_start_mission(mids[i]))
    bench("T15", lambda i: q.t15_active_missions(pick(refs["depots"], i)))
    bench("T7_end", lambda i: q.t7_end_mission(mids[i], utcnow() + dt.timedelta(minutes=45)))

    bench("T9", lambda i: q.t9_record_event(pick(refs["drivers"], i), pick(refs["plates"], i),
                                            "SPEEDING", utcnow(), 118.0, 33.6, -7.5))
    bench("T10", lambda i: q.t10_driver_week(*pick(refs["weeks"], i)))
    parts = [PartItem("Filtre à huile", "FLT-OIL", 1, Decimal("90"))]
    bench("T11", lambda i: q.t11_record_maintenance(
        pick(refs["plates"], i), utcnow().date(), 150000 + i, "VIDANGE", parts,
        Decimal("890.00")))
    bench("T12", lambda i: q.t12_add_equipment(pick(refs["plates"], i), "TEST_EQUIP"))

    # T14 : supprime les positions de BULK-0001 sur 2 jours, vérifie avant/après
    start = base.replace(hour=0)
    end = start + dt.timedelta(days=1, hours=23)
    before = q.count_positions("BULK-0001", start, end)
    bench("T14", lambda i: q.t14_delete_range("BULK-0001", start, end))
    after = q.count_positions("BULK-0001", start, end)

    # T13 : démonstration (contient une pause, donc non chronométrée)
    ttl = q.t13_ttl_demo(*refs["traces"][0])

    # ------------------------------------------------------------ rapport
    by = {}
    for r in results:
        by.setdefault(r["label"], []).append(r["ms"])
    print(f"\n=== Résultats (CL={cl_name}, {n} exécutions) ===")
    print(f"{'requête':<10}{'médiane ms':>12}{'min ms':>10}{'max ms':>10}")
    for label, v in by.items():
        print(f"{label:<10}{statistics.median(v):>12.2f}{min(v):>10.2f}{max(v):>10.2f}")
    print(f"\nT14 : positions de BULK-0001 avant = {before}, après = {after}")
    print(f"T13 : {ttl}")

    out_dir = os.path.join("benchmarks", "results")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"transactional_{cl_name}_{time.strftime('%Y%m%d_%H%M%S')}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["query", "consistency", "run", "ms"])
        counters = {}
        for r in results:
            counters[r["label"]] = counters.get(r["label"], 0) + 1
            w.writerow([r["label"], cl_name, counters[r["label"]], f"{r['ms']:.3f}"])
    print(f"Mesures enregistrées dans {path}")
    cluster.shutdown()


if __name__ == "__main__":
    main()
