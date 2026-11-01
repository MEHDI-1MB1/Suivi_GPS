#!/usr/bin/env python3
"""Test de cohérence Cassandra (rôle A) : ONE / QUORUM / ALL, avec et sans panne.

PRÉREQUIS : cluster à 3 nœuds, keyspace en RF=3 (profil full), services cassandra1..3 démarrés.

  python -m queries.consistency                 # phases 1, 2 et 3
  python -m queries.consistency --two-down      # ajoute la phase 2b : deux nœuds arrêtés
  python -m queries.consistency --n 300         # plus de mesures

Phases :
  1. Tous les nœuds actifs : latence d'écriture et de lecture pour ONE, QUORUM, ALL.
  2. cassandra3 arrêté     : quels niveaux continuent de répondre ? (ALL doit échouer)
  2b. (option) cassandra2 aussi arrêté : seul ONE doit répondre.
  3. Lecture périmée       : écritures en ONE pendant la panne, redémarrage de cassandra3, puis
                             lecture immédiate sur ce nœud en ONE, puis en QUORUM (read repair).

Le script pilote Docker (docker stop / docker start) : lancez-le depuis WSL, dans le dossier du projet.
Les données de test utilisent une plaque "CONS-xxxx" avec un TTL de 1 h.
"""
import argparse
import collections
import csv
import datetime as dt
import os
import shutil
import statistics
import subprocess
import time
import uuid

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy

from common.db import connect

LEVELS = ["ONE", "QUORUM", "ALL"]
NODES = ["cassandra1", "cassandra2", "cassandra3"]


# ------------------------------------------------------------------ Docker / cluster
def sh(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def container_ip(name):
    r = sh("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", name)
    return r.stdout.strip()


def hosts_up(cluster):
    return sum(1 for h in cluster.metadata.all_hosts() if h.is_up)


def wait_until(cond, timeout, label):
    start = time.time()
    while time.time() - start < timeout:
        if cond():
            return True
        time.sleep(2)
    print(f"  ! délai dépassé en attendant : {label}")
    return False


def stop_node(name, cluster, expected_up):
    print(f"  -> docker stop {name}")
    sh("docker", "stop", name)
    wait_until(lambda: hosts_up(cluster) == expected_up, 90, f"{expected_up} nœuds vus actifs par le driver")


def start_node(name):
    print(f"  -> docker start {name}")
    sh("docker", "start", name)


def connect_via(ip, keyspace="fleet", timeout=240):
    """Session dont le coordinateur est forcément le nœud d'adresse ip (liste blanche)."""
    profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([ip]), request_timeout=20)
    start = time.time()
    while time.time() - start < timeout:
        try:
            cl = Cluster([ip], port=9042, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
            return cl, cl.connect(keyspace)
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"impossible de se connecter à {ip}")


# ------------------------------------------------------------------ sonde de lecture / écriture
class Probe:
    def __init__(self, session):
        self.s = session
        self.plate = "CONS-" + uuid.uuid4().hex[:8]
        self.base = dt.datetime.combine(dt.date.today(), dt.time(1, 0))
        self.day = self.base.date()
        self.k = 0
        self.w = session.prepare(
            "INSERT INTO positions_by_vehicle_day (plate,day,ts,lat,lon,speed_kmh,heading,fuel_pct,"
            "engine_state) VALUES (?,?,?,?,?,?,?,?,?) USING TTL 3600")
        self.r = session.prepare("SELECT ts, lat FROM positions_by_vehicle_day WHERE plate=? AND day=? AND ts=?")

    def write_new(self, cl):
        self.k += 1
        ts = self.base + dt.timedelta(seconds=self.k)
        self.w.consistency_level = getattr(ConsistencyLevel, cl)
        self.s.execute(self.w, (self.plate, self.day, ts, 33.5, -7.6, 50.0, 90, 60.0, "ON"))
        return ts

    def read(self, cl, ts, session=None, stmt=None):
        stmt = stmt or self.r
        stmt.consistency_level = getattr(ConsistencyLevel, cl)
        return (session or self.s).execute(stmt, (self.plate, self.day, ts)).one()


def measure(fn, n):
    lat, errs, ok = [], collections.Counter(), 0
    for i in range(n):
        t = time.perf_counter()
        try:
            fn(i)
            ok += 1
            lat.append((time.perf_counter() - t) * 1000)
        except Exception as e:  # Unavailable, Timeout, NoHostAvailable...
            errs[type(e).__name__] += 1
    return ok, lat, errs


def pct(lat, q):
    if not lat:
        return None
    s = sorted(lat)
    return s[min(len(s) - 1, int(q * len(s)))]


def record(rows, phase, cl, op, n, ok, lat, errs):
    rows.append(dict(phase=phase, cl=cl, op=op, n=n, ok=ok, failed=n - ok,
                     errors=";".join(f"{k}x{v}" for k, v in errs.items()),
                     median_ms=round(statistics.median(lat), 2) if lat else "",
                     p95_ms=round(pct(lat, 0.95), 2) if lat else ""))


def show(rows, phase):
    print(f"\n--- {phase} ---")
    print(f"{'CL':<8}{'op':<9}{'ok/n':>9}{'médiane ms':>13}{'p95 ms':>10}   erreurs")
    for r in rows:
        if r["phase"] == phase:
            print(f"{r['cl']:<8}{r['op']:<9}{str(r['ok']) + '/' + str(r['n']):>9}"
                  f"{str(r['median_ms']):>13}{str(r['p95_ms']):>10}   {r['errors']}")


def run_levels(p, rows, phase, n, pool):
    for cl in LEVELS:
        written = []
        ok, lat, errs = measure(lambda i: written.append(p.write_new(cl)), n)
        record(rows, phase, cl, "écriture", n, ok, lat, errs)
        ok, lat, errs = measure(lambda i: p.read(cl, pool[i % len(pool)]), n)
        record(rows, phase, cl, "lecture", n, ok, lat, errs)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=200, help="opérations par niveau et par type")
    ap.add_argument("--two-down", action="store_true", help="phase 2b : arrêter aussi cassandra2")
    args = ap.parse_args()
    if not shutil.which("docker"):
        raise SystemExit("La commande docker est introuvable : lancez ce script depuis WSL.")

    cluster, s = connect("ONE")
    rf = s.execute("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name='fleet'").one().replication
    print(f"Nœuds vus actifs : {hosts_up(cluster)}   réplication : {dict(rf)}")
    if hosts_up(cluster) < 3 or str(rf.get("dc1")) != "3":
        raise SystemExit("Il faut 3 nœuds actifs et RF=3 (profil full, schéma chargé sans le sed RF=1).")

    p = Probe(s)
    rows, n = [], args.n

    # ---------------- Phase 1 : tous les nœuds actifs
    print("\n== Phase 1 : 3 nœuds actifs ==")
    seed = []
    for _ in range(50):                       # lignes présentes sur tous les réplicas (écrites en ALL)
        seed.append(p.write_new("ALL"))
    run_levels(p, rows, "1 - 3 nœuds actifs", n, seed)
    show(rows, "1 - 3 nœuds actifs")

    # ---------------- Phase 2 : un nœud arrêté
    print("\n== Phase 2 : cassandra3 arrêté ==")
    stop_node("cassandra3", cluster, 2)
    run_levels(p, rows, "2 - cassandra3 arrêté", min(n, 50), seed)
    show(rows, "2 - cassandra3 arrêté")

    # ---------------- Phase 3 (écritures pendant la panne)
    print("\n== Phase 3 : lecture périmée après la panne ==")
    during = [p.write_new("ONE") for _ in range(200)]
    print(f"  {len(during)} lignes écrites en ONE pendant que cassandra3 est arrêté")

    # ---------------- Phase 2b : deux nœuds arrêtés (optionnel)
    if args.two_down:
        print("\n== Phase 2b : cassandra2 ET cassandra3 arrêtés ==")
        stop_node("cassandra2", cluster, 1)
        run_levels(p, rows, "2b - 2 nœuds arrêtés", min(n, 50), seed)
        show(rows, "2b - 2 nœuds arrêtés")
        start_node("cassandra2")
        wait_until(lambda: hosts_up(cluster) == 2, 180, "cassandra2 actif")

    # ---------------- Phase 3 : redémarrage et lecture immédiate sur cassandra3
    start_node("cassandra3")
    ip3 = container_ip("cassandra3")
    c3, s3 = connect_via(ip3)
    r3 = s3.prepare(p.r.query_string)
    stale = {}
    for cl in ("ONE", "QUORUM", "ONE"):
        seen = sum(1 for ts in during if p.read(cl, ts, session=s3, stmt=r3) is not None)
        label = f"{cl}{' (après QUORUM)' if cl == 'ONE' and 'QUORUM' in stale else ''}"
        stale[label] = seen
    print("  lignes visibles via cassandra3 juste après son redémarrage :")
    for label, seen in stale.items():
        print(f"    lecture {label:<20} -> {seen}/{len(during)}")
    for label, seen in stale.items():
        rows.append(dict(phase="3 - lecture périmée", cl=label, op="lecture", n=len(during), ok=seen,
                         failed=len(during) - seen, errors="lignes manquantes", median_ms="", p95_ms=""))

    # ---------------- retour à l'état normal
    c3.shutdown()
    wait_until(lambda: hosts_up(cluster) == 3, 180, "3 nœuds actifs")

    os.makedirs(os.path.join("benchmarks", "results"), exist_ok=True)
    path = os.path.join("benchmarks", "results", f"consistency_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"\nRésultats enregistrés dans {path}")
    cluster.shutdown()


if __name__ == "__main__":
    main()
