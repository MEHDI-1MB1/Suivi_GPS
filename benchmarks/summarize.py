#!/usr/bin/env python3
"""Synthèse des mesures : regroupe tous les CSV d'un type et calcule, par requête et par
niveau de cohérence, la médiane, le p95 et le rapport à ONE.

  python benchmarks/summarize.py --prefix analytics
  python benchmarks/summarize.py --prefix transactional --since 20261101_155000

--since ignore les fichiers plus anciens (format AAAAMMJJ_HHMMSS, celui du nom des CSV),
utile pour écarter les mesures faites avant un rechargement des données.
Écrit aussi benchmarks/results/summary_<prefix>.csv (utilisable par le tableau de bord).
"""
import argparse
import glob
import os
import re

import pandas as pd

LEVELS = ["ONE", "QUORUM", "ALL"]


def natural(q):
    m = re.match(r"([A-Za-z]+)(\d+)(.*)", q)
    return (m.group(1), int(m.group(2)), m.group(3)) if m else (q, 0, "")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prefix", required=True, choices=["analytics", "transactional"])
    ap.add_argument("--since", default="", help="AAAAMMJJ_HHMMSS")
    ap.add_argument("--dir", default=os.path.join("benchmarks", "results"))
    args = ap.parse_args()

    frames, used = [], 0
    for f in sorted(glob.glob(os.path.join(args.dir, f"{args.prefix}_*.csv"))):
        m = re.search(r"_(\d{8}_\d{6})\.csv$", f)
        if not m or m.group(1) < args.since:
            continue
        df = pd.read_csv(f)
        if "status" in df.columns:
            df = df[df["status"].fillna("ok") == "ok"]
        df["ms"] = pd.to_numeric(df["ms"], errors="coerce")
        frames.append(df.dropna(subset=["ms"])[["query", "consistency", "ms"]])
        used += 1
    if not frames:
        raise SystemExit("Aucun fichier retenu.")
    d = pd.concat(frames)
    g = d.groupby(["query", "consistency"])["ms"]
    med = g.median().unstack("consistency")
    p95 = g.quantile(0.95).unstack("consistency")
    cnt = g.size().unstack("consistency")
    cols = [c for c in LEVELS if c in med.columns]
    order = sorted(med.index, key=natural)

    out = med.loc[order, cols].round(2).copy()
    out.columns = [f"{c} méd." for c in cols]
    for c in cols[1:]:
        out[f"{c}/ONE"] = (med.loc[order, c] / med.loc[order, "ONE"]).round(2) if "ONE" in cols else None
    for c in cols:
        out[f"{c} p95"] = p95.loc[order, c].round(2)
    print(f"{used} fichier(s), {len(d)} mesures. Valeurs en ms ; 'X/ONE' = médiane de X divisée par celle de ONE.\n")
    print(out.to_string())
    print("\nMesures par cellule (min - max) :", int(cnt.min().min()), "-", int(cnt.max().max()))

    out.reset_index().rename(columns={"query": "requête"}).to_csv(
        os.path.join(args.dir, f"summary_{args.prefix}.csv"), index=False)
    print(f"\nÉcrit : {os.path.join(args.dir, 'summary_' + args.prefix + '.csv')}")


if __name__ == "__main__":
    main()
