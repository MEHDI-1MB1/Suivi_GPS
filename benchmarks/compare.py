"""Tableau natif contre Spark (médiane et p95), à partir des CSV de benchmarks/results/.

  python benchmarks/compare.py                       # derniers fichiers
  python benchmarks/compare.py <analytics_ONE_x.csv>  # fichier natif précis

Natif : le fichier analytics_ONE_*.csv (le plus récent par défaut).
Spark : tous les spark_bench_*.csv ; pour une même requête, le fichier le plus récent l'emporte.
Écrit benchmarks/results/comparison.csv.
"""
import csv
import glob
import os
import statistics
import sys
from collections import defaultdict

DIR = os.getenv("RESULTS_DIR", "benchmarks/results")


def load(paths, default_mode):
    out = {}
    for path in paths:                       # ordre chronologique : le dernier écrase
        d = defaultdict(list)
        for r in csv.DictReader(open(path)):
            d[(r["query"], r.get("mode") or default_mode)].append(float(r["ms"]))
        out.update(d)
    return out


def p95(v):
    s = sorted(v)
    return s[min(len(s) - 1, int(0.95 * len(s)))]


native_files = [sys.argv[1]] if len(sys.argv) > 1 else sorted(glob.glob(f"{DIR}/analytics_ONE_*.csv"))[-1:]
spark_files = sorted(glob.glob(f"{DIR}/spark_bench_*.csv"))
if not native_files or not spark_files:
    sys.exit("Fichiers introuvables : il faut un analytics_ONE_*.csv et au moins un spark_bench_*.csv")

nat = {q: v for (q, m), v in load(native_files, "native").items()}
spk = {q: v for (q, m), v in load(spark_files, "spark_cluster").items()}

rows = []
print(f"{'requête':<8}{'natif méd.':>12}{'natif p95':>11}{'spark méd.':>12}{'spark p95':>11}{'spark/natif':>13}")
for q in sorted(set(nat) | set(spk)):
    n, s = nat.get(q), spk.get(q)
    nm = statistics.median(n) if n else None
    sm = statistics.median(s) if s else None
    ratio = f"x{sm / nm:.0f}" if n and s else "-"
    print(f"{q:<8}{(f'{nm:.1f}' if n else '-'):>12}{(f'{p95(n):.1f}' if n else '-'):>11}"
          f"{(f'{sm:.0f}' if s else '-'):>12}{(f'{p95(s):.0f}' if s else '-'):>11}{ratio:>13}")
    rows.append({"query": q,
                 "native_median_ms": round(nm, 2) if n else "", "native_p95_ms": round(p95(n), 2) if n else "",
                 "spark_median_ms": round(sm, 1) if s else "", "spark_p95_ms": round(p95(s), 1) if s else ""})

with open(f"{DIR}/comparison.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0]))
    w.writeheader()
    w.writerows(rows)
print(f"\nnatif : {', '.join(os.path.basename(p) for p in native_files)}")
print(f"spark : {', '.join(os.path.basename(p) for p in spark_files)}")
print(f"-> {DIR}/comparison.csv")