"""Harnais de benchmark : 1 exécution de chauffe, puis N exécutions mesurées.

Format des CSV, aligné sur celui de A avec une colonne "mode" en plus :
    query,mode,consistency,run,ms
    (mode : native | spark_cluster | spark_local ; consistency : ONE / QUORUM / ALL ou vide)

La fonction passée à benchmark() doit exécuter réellement la requête
(pour Spark : finir par .collect() ou .count(), sinon rien n'est mesuré).
"""
import csv
import os
import statistics
import time
from datetime import datetime, timezone

RESULTS_DIR = os.getenv("RESULTS_DIR", "benchmarks/results")
PREFIX = os.getenv("BENCH_PREFIX", "spark")
STAMP = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
OUT_FILE = os.path.join(RESULTS_DIR, f"{PREFIX}_{STAMP}.csv")
FIELDS = ["query", "mode", "consistency", "run", "ms"]


def _append(rows):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    new = not os.path.exists(OUT_FILE)
    with open(OUT_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if new:
            w.writeheader()
        w.writerows(rows)


def benchmark(query, fn, mode="spark_cluster", consistency="", warmup=1, runs=10):
    """Mesure fn() : `warmup` exécutions ignorées, puis `runs` exécutions enregistrées."""
    for _ in range(warmup):
        fn()
    rows, durations = [], []
    for i in range(1, runs + 1):
        t0 = time.perf_counter()
        fn()
        ms = (time.perf_counter() - t0) * 1000
        durations.append(ms)
        rows.append({"query": query, "mode": mode, "consistency": consistency,
                     "run": i, "ms": round(ms, 3)})
    _append(rows)
    s = sorted(durations)
    p95 = s[min(len(s) - 1, int(0.95 * len(s)))]
    stats = {"median": statistics.median(s), "mean": statistics.mean(s), "p95": p95}
    print(f"{query} [{mode} {consistency}] median={stats['median']:.0f} ms "
          f"mean={stats['mean']:.0f} ms p95={p95:.0f} ms  -> {OUT_FILE}")
    return stats