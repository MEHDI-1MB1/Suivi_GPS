"""Effet de l'indexation sur "véhicules d'un dépôt".

Trois façons de répondre à la même question (WHERE depot_id = ?) :
  IDX_scan   sans index : ALLOW FILTERING, balayage de toute la table vehicles
                          (uniquement avec --with-drop : l'index est supprimé puis recréé)
  IDX_index  index secondaire vehicles_depot_idx
  IDX_table  table dédiée vehicles_by_depot (clé de partition = depot_id)

Depuis la racine du dépôt :
  python benchmarks/index_effect.py --repeat 30
  python benchmarks/index_effect.py --repeat 30 --with-drop --cl QUORUM

Écrit benchmarks/results/index_<CL>_<horodatage>.csv  (query,consistency,run,ms).
"""
import argparse
import csv
import os
import statistics
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.getcwd())
os.environ.setdefault("CASSANDRA_HOSTS", "127.0.0.1")
os.environ.setdefault("CASSANDRA_SINGLE_HOST", "1")

from common.db import connect  # noqa: E402

COLS = "plate, vehicle_type, model"
Q_SCAN = f"SELECT {COLS} FROM vehicles WHERE depot_id = ? ALLOW FILTERING"
Q_IDX = f"SELECT {COLS} FROM vehicles WHERE depot_id = ?"
Q_TAB = f"SELECT {COLS} FROM vehicles_by_depot WHERE depot_id = ?"


def timeit(session, stmt, depot, repeat, warmup=3):
    for _ in range(warmup):
        rows = list(session.execute(stmt, (depot,)))
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        rows = list(session.execute(stmt, (depot,)))
        times.append((time.perf_counter() - t0) * 1000)
    return times, len(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repeat", type=int, default=30)
    ap.add_argument("--depot", default="DEP01")
    ap.add_argument("--cl", default="ONE")
    ap.add_argument("--with-drop", action="store_true",
                    help="mesure aussi sans index (supprime puis recrée vehicles_depot_idx)")
    args = ap.parse_args()

    cluster, s = connect(args.cl)
    results = {}
    try:
        if args.with_drop:
            s.execute("DROP INDEX IF EXISTS vehicles_depot_idx")
            time.sleep(2)
            results["IDX_scan"] = timeit(s, s.prepare(Q_SCAN), args.depot, args.repeat)
            s.execute("CREATE INDEX IF NOT EXISTS vehicles_depot_idx ON vehicles (depot_id)")
            expected = results["IDX_scan"][1]
            q_idx = s.prepare(Q_IDX)
            for _ in range(60):                      # attendre la fin de la construction de l'index
                if len(list(s.execute(q_idx, (args.depot,)))) == expected:
                    break
                time.sleep(1)
        results["IDX_index"] = timeit(s, s.prepare(Q_IDX), args.depot, args.repeat)
        results["IDX_table"] = timeit(s, s.prepare(Q_TAB), args.depot, args.repeat)
    finally:
        # l'index doit exister à la fin, même si une mesure a échoué
        s.execute("CREATE INDEX IF NOT EXISTS vehicles_depot_idx ON vehicles (depot_id)")
        cluster.shutdown()

    counts = {k: v[1] for k, v in results.items()}
    print(f"dépôt {args.depot} | CL={args.cl.upper()} | lignes renvoyées : {counts}")
    if len(set(counts.values())) > 1:
        print("ATTENTION : les variantes ne renvoient pas le même nombre de lignes.")
    print(f"{'variante':<12}{'médiane ms':>12}{'p95 ms':>10}{'min ms':>10}{'max ms':>10}")
    for name, (times, _) in results.items():
        t = sorted(times)
        p95 = t[min(len(t) - 1, int(0.95 * len(t)))]
        print(f"{name:<12}{statistics.median(t):>12.2f}{p95:>10.2f}{t[0]:>10.2f}{t[-1]:>10.2f}")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = f"benchmarks/results/index_{args.cl.upper()}_{stamp}.csv"
    os.makedirs("benchmarks/results", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["query", "consistency", "run", "ms"])
        for name, (times, _) in results.items():
            for i, ms in enumerate(times, 1):
                w.writerow([name, args.cl.upper(), i, round(ms, 3)])
    print("->", path)


if __name__ == "__main__":
    main()