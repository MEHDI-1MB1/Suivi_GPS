"""Compare les résultats de S28, S29 et S30 à la vérité terrain de A.

Depuis la racine du dépôt :  python spark-jobs/validate.py
(lit spark-jobs/out/*.json et data/ground_truth.json)
"""
import json
from pathlib import Path

OUT = Path("spark-jobs/out")
GT = json.load(open("data/ground_truth.json"))


def load(name):
    path = OUT / f"{name}.json"
    if not path.exists():
        print(f"  {name}.json introuvable : lancez d'abord le job.")
        return None
    return json.load(open(path))


def compare(label, expected, found):
    expected, found = set(expected), set(found)
    print(f"  attendus {len(expected)} | trouvés {len(found)} | "
          f"retrouvés {len(expected & found)} | manquants {len(expected - found)} | "
          f"en trop {len(found - expected)}")
    for x in sorted(expected - found)[:5]:
        print(f"    manquant : {x}")
    for x in sorted(found - expected)[:5]:
        print(f"    en trop  : {x}")
    print(f"  => {label} : {'OK' if expected == found else 'ECART'}\n")


print("Clés de ground_truth.json :", list(GT.keys()), "\n")
stops = GT.get("long_stops", [])

print("S28 : arrêts > 30 min (comparaison par mission_id)")
r = load("S28")
if r is not None:
    compare("S28", [s["mission_id"] for s in stops], [x["mission_id"] for x in r])
    print("  durées : minutes injectées par A vs durée mesurée par Spark")
    measured = {x["mission_id"]: x["duration_min"] for x in r}
    for s in stops:
        print(f"    {s['plate']:<12} injectée {s['minutes']:>3} min | Spark {measured.get(s['mission_id'], '-')}")
    print()

print("S30 : vols de carburant (comparaison par plaque)")
r = load("S30")
if r is not None:
    compare("S30", [s["plate"] for s in stops if s.get("fuel_theft")], [x["plate"] for x in r])

print("S29 : sauts GPS")
r = load("S29")
if r is not None:
    keys = [k for k in GT if "jump" in k.lower() or "gps" in k.lower()]
    if keys:
        gj = GT[keys[0]]
        print(f"  (clé de la vérité terrain : {keys[0]})")
        print(f"  attendus {len(gj)} | trouvés {len(r)}")
        print(f"  exemple de la vérité terrain : {gj[:1]}")
        if gj and isinstance(gj[0], dict) and "plate" in gj[0]:
            from collections import Counter
            same = Counter(g["plate"] for g in gj) == Counter(x["plate"] for x in r)
            print(f"  mêmes plaques (avec multiplicité) : {same}")
        print()
    else:
        print(f"  pas de clé 'jump' dans la vérité terrain ; A annonce 51 sauts, "
              f"S29 en trouve {len(r)}\n")