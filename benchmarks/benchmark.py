import csv
import os
import statistics
import time
from datetime import datetime, timezone

RESULTS_FILE = os.getenv("RESULTS_FILE", "results/benchmarks.csv")
FIELDS = ["query_id", "mode", "consistency", "run", "duration_ms", "ts"]


def _append(rows):
    os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
    new = not os.path.exists(RESULTS_FILE)
    with open(RESULTS_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if new:
            w.writeheader()
        w.writerows(rows)


def benchmark(query_id, fn, mode="native", consistency="", warmup=1, runs=10):
    """Exécute fn() 1 fois pour chauffer puis `runs` fois, enregistre chaque durée.
    fn doit exécuter réellement la requête (pour Spark : finir par .collect() ou .count())."""
    for _ in range(warmup):
        fn()
    rows, durations = [], []
    for i in range(1, runs + 1):
        t0 = time.perf_counter()
        fn()
        ms = (time.perf_counter() - t0) * 1000
        durations.append(ms)
        rows.append({"query_id": query_id, "mode": mode, "consistency": consistency,
                     "run": i, "duration_ms": round(ms, 3),
                     "ts": datetime.now(timezone.utc).isoformat()})
    _append(rows)
    durations.sort()
    p95 = durations[min(len(durations) - 1, int(0.95 * len(durations)))]
    print(f"{query_id} [{mode} {consistency}] median={statistics.median(durations):.1f} ms "
          f"mean={statistics.mean(durations):.1f} ms p95={p95:.1f} ms")


if __name__ == "__main__":
    benchmark("DEMO", lambda: sum(range(1_000_000)), mode="test")