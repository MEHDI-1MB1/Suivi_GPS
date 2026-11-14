"""Tableaux de mesures à partir des CSV de benchmarks/results/.

  python benchmarks/compare.py                          # derniers fichiers de chaque famille
  python benchmarks/compare.py <analytics_ONE_x.csv>    # fichier natif A précis pour comparison.csv

Sorties :
  comparison.csv   natif contre Spark, requêtes présentes dans les deux modes (A16-A19...)
  all_queries.csv  les 30 requêtes : médiane, p95, nombre de mesures et d'erreurs
  consistency.csv  médiane par niveau de cohérence (ONE / QUORUM / ALL)

Règles : les lignes dont la colonne "status" vaut autre chose que "ok" sont comptées comme
erreurs et exclues des médianes ; pour une même requête, le fichier le plus récent l'emporte.
"""
import csv
import glob
import os
import re
import statistics
import sys
from collections import defaultdict

DIR = os.getenv("RESULTS_DIR", "benchmarks/results")
LEVELS = ["ONE", "QUORUM", "ALL"]


def load(paths, default_mode="native"):
    """{(requête, mode): {"ms": [...], "errors": n}} ; le dernier fichier écrase."""
    out = {}
    for path in paths:
        d = defaultdict(lambda: {"ms": [], "errors": 0})
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                key = (r["query"], r.get("mode") or default_mode)
                ok = (r.get("status") or "ok").strip().lower() == "ok"
                try:
                    ms = float(r["ms"])
                except (TypeError, ValueError):
                    ms, ok = None, False
                if ok:
                    d[key]["ms"].append(ms)
                else:
                    d[key]["errors"] += 1
        out.update(d)
    return out


def p95(v):
    s = sorted(v)
    return s[min(len(s) - 1, int(0.95 * len(s)))]


def med(v):
    return statistics.median(v) if v else None


def latest(pattern, n=1):
    return sorted(glob.glob(os.path.join(DIR, pattern)))[-n:]


def order(q):
    m = re.match(r"([TAS])(\d+)(.*)", q)
    return ("TAS".index(m.group(1)), int(m.group(2)), m.group(3)) if m else (9, 0, q)


def write(name, rows):
    with open(os.path.join(DIR, name), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def fmt(x, nd=1):
    return "-" if x is None else f"{x:.{nd}f}"


native_files = [sys.argv[1]] if len(sys.argv) > 1 else latest("analytics_ONE_*.csv")
spark_files = sorted(glob.glob(os.path.join(DIR, "spark_bench_*.csv")))
trans_files = latest("transactional_ONE_*.csv")
if not native_files or not spark_files:
    sys.exit("Il faut au moins un analytics_ONE_*.csv et un spark_bench_*.csv")

nat = {q: v for (q, m), v in load(native_files).items()}
spk = {q: v for (q, m), v in load(spark_files, "spark_cluster").items()}
trn = {q: v for (q, m), v in load(trans_files).items()}

# ---------------------------------------------------------------- natif contre Spark
print("=== Natif contre Spark ===")
print(f"{'requête':<8}{'natif méd.':>12}{'natif p95':>11}{'spark méd.':>12}{'spark p95':>11}{'spark/natif':>13}")
rows = []
for q in sorted(set(nat) & set(spk), key=order):
    n, s = nat[q]["ms"], spk[q]["ms"]
    if not n or not s:
        continue
    print(f"{q:<8}{fmt(med(n)):>12}{fmt(p95(n)):>11}{fmt(med(s), 0):>12}{fmt(p95(s), 0):>11}{'x%.0f' % (med(s) / med(n)):>13}")
    rows.append({"query": q, "native_median_ms": round(med(n), 2), "native_p95_ms": round(p95(n), 2),
                 "spark_median_ms": round(med(s), 1), "spark_p95_ms": round(p95(s), 1)})
if rows:
    write("comparison.csv", rows)

# ---------------------------------------------------------------- les 30 requêtes
all_rows = []
for source, data in (("Cassandra (T)", trn), ("Cassandra (A)", nat), ("Spark (S)", spk)):
    for q, v in data.items():
        if (source == "Cassandra (A)" and not q.startswith("A")) or \
           (source == "Cassandra (T)" and not q.startswith("T")) or \
           (source == "Spark (S)" and not q.startswith("S")):
            continue
        ms = v["ms"]
        all_rows.append({"query": q, "source": source, "runs": len(ms), "errors": v["errors"],
                         "median_ms": round(med(ms), 2) if ms else "", "p95_ms": round(p95(ms), 2) if ms else ""})
all_rows.append({"query": "T13", "source": "Cassandra (T)", "runs": 0, "errors": 0,
                 "median_ms": "", "p95_ms": ""})        # TTL de 60 jours : propriété du schéma, non chronométrée
all_rows.sort(key=lambda r: order(r["query"]))
write("all_queries.csv", all_rows)
measured = [r for r in all_rows if r["runs"]]
print(f"\n=== Les 30 requêtes : {len(measured)} mesurées ===")
missing = [f"T{i}" for i in range(1, 16)
           if not any((r["query"] == f"T{i}" or r["query"].startswith(f"T{i}_")) and r["runs"] for r in all_rows)]
missing += [f"A{i}" for i in range(16, 26) if not any(r["query"] == f"A{i}" and r["runs"] for r in all_rows)]
missing += [f"S{i}" for i in range(26, 31) if not any(r["query"] == f"S{i}" and r["runs"] for r in all_rows)]
print("sans mesure :", ", ".join(missing) or "aucune",
      "(T13 = TTL, non chronométrée ; T7 est mesurée en deux temps : T7_start, T7_end)")
errs = [f"{r['query']}:{r['errors']}" for r in all_rows if r["errors"]]
if errs:
    print("exécutions en erreur :", ", ".join(errs))

# ---------------------------------------------------------------- cohérence
by_cl = {}
for family in ("transactional", "analytics"):
    for lvl in LEVELS:
        files = latest(f"{family}_{lvl}_*.csv")
        if files:
            for (q, m), v in load(files).items():
                by_cl.setdefault(q, {})[lvl] = med(v["ms"])
rows_cl = []
for q in sorted(by_cl, key=order):
    if len(by_cl[q]) >= 2:
        rows_cl.append({"query": q, **{f"{l}_ms": (round(by_cl[q][l], 2) if by_cl[q].get(l) is not None else "")
                                       for l in LEVELS}})
if rows_cl:
    write("consistency.csv", rows_cl)
    print("\n=== Médiane par niveau de cohérence (ms) ===")
    print(f"{'requête':<10}" + "".join(f"{l:>10}" for l in LEVELS))
    for r in rows_cl:
        print(f"{r['query']:<10}" + "".join(f"{(str(r[l + '_ms']) or '-'):>10}" for l in LEVELS))

print(f"\nnatif A : {', '.join(map(os.path.basename, native_files))}")
print(f"natif T : {', '.join(map(os.path.basename, trans_files))}")
print(f"spark   : {', '.join(map(os.path.basename, spark_files))}")