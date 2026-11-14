"""Tableau de bord de la flotte : Cassandra (natif) + résultats Spark + benchmarks.

Depuis la racine du dépôt, avec le cluster Cassandra démarré :
    streamlit run dashboard/app.py

Sources :
  - Cassandra : last_position_by_vehicle, vehicles et les analytiques natives A16-A25
    (réutilise queries/analytics.py de A, sans dupliquer ses requêtes)
  - spark-jobs/out/S27..S30.json : résultats des jobs Spark (s26_s30_cassandra.py)
  - benchmarks/results/comparison.csv : tableau natif contre Spark (compare.py)
"""
import json
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("CASSANDRA_HOSTS", "127.0.0.1")
os.environ.setdefault("CASSANDRA_SINGLE_HOST", "1")

import numpy as np
import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st

import queries.analytics as qa
from common.db import connect

# Date de référence du jeu de données (même valeur que --now au chargement)
REF_NOW = datetime.fromisoformat(os.getenv("REF_NOW", "2026-10-31T12:00:00"))
qa._NOW = REF_NOW
SPARK_OUT = ROOT / "spark-jobs" / "out"
COMPARISON = ROOT / "benchmarks" / "results" / "comparison.csv"

STATES = {"moving": ("En route", [31, 157, 138]),
          "stopped": ("À l'arrêt", [224, 122, 40]),
          "silent": ("Sans position > 1 h", [140, 150, 160])}

st.set_page_config(page_title="Flotte", page_icon="🚚", layout="wide")


# ------------------------------------------------------------------ accès aux données
@st.cache_resource
def get_api():
    cluster, session = connect("ONE")
    return qa.Analytics(session), session


@st.cache_data(ttl=120)
def native():
    """Analytiques natives de A (tables de compteurs et petits balayages)."""
    api, _ = get_api()
    a22 = [{"day": d, "type": t, "count": c}
           for d, by_type in api.a22_events_per_day().items() for t, c in by_type.items()]
    return {
        "a16": pd.DataFrame(api.a16_missions_per_day(), columns=["day", "missions", "km"]),
        "a17": pd.DataFrame(api.a17_top_speeders(), columns=["driver_id", "name", "speeding"]),
        "a18": pd.DataFrame(api.a18_late_rate(), columns=["depot", "done", "late", "late_pct"]),
        "a19": pd.DataFrame(api.a19_avg_speed_by_type(), columns=["type", "avg_kmh", "samples"]),
        "a21": pd.DataFrame(api.a21_silent_vehicles(), columns=["plate", "last_ts"]),
        "a22": pd.DataFrame(a22, columns=["day", "type", "count"]),
        "a23": pd.DataFrame(api.a23_avg_fuel_by_model(), columns=["model", "avg_fuel", "samples"]),
        "a24": len(api.a24_overdue_maintenance()),
        "a25": pd.DataFrame(api.a25_avg_duration_by_depot(), columns=["depot", "minutes", "done"]),
    }


@st.cache_data(ttl=60)
def last_positions():
    _, s = get_api()
    pos = pd.DataFrame(list(s.execute(
        "SELECT plate, ts, lat, lon, speed_kmh, fuel_pct, engine_state FROM last_position_by_vehicle")))
    veh = pd.DataFrame(list(s.execute("SELECT plate, vehicle_type, depot_id FROM vehicles")))
    df = pos.merge(veh, on="plate", how="left")
    age_h = (REF_NOW - df["ts"]).dt.total_seconds() / 3600
    df["state"] = np.where(age_h > 1, "silent", np.where(df["speed_kmh"] >= 1, "moving", "stopped"))
    df["label"] = df["state"].map(lambda k: STATES[k][0])
    df["color"] = df["state"].map(lambda k: STATES[k][1])
    df["time"] = df["ts"].dt.strftime("%d/%m %H:%M")
    return df


def spark_result(name):
    path = SPARK_OUT / f"{name}.json"
    return pd.DataFrame(json.load(open(path))) if path.exists() else None


def need_spark(name):
    st.info(f"Résultat {name} introuvable : lancez `s26_s30_cassandra.py {name}` sur le cluster Spark.")


# ------------------------------------------------------------------ connexion
try:
    get_api()
except Exception as exc:                                   # Cassandra arrêtée, mauvais port...
    st.error(f"Connexion à Cassandra impossible : {exc}")
    st.caption("Vérifiez `docker exec cassandra1 nodetool status` (nœuds UN).")
    st.stop()

nat, pos = native(), last_positions()

# ------------------------------------------------------------------ barre latérale
st.sidebar.header("Filtres de la carte")
depots = sorted(pos["depot_id"].dropna().unique())
types = sorted(pos["vehicle_type"].dropna().unique())
sel_depots = st.sidebar.multiselect("Dépôts", depots, default=depots)
sel_types = st.sidebar.multiselect("Types de véhicule", types, default=types)
if st.sidebar.button("Actualiser les données"):
    st.cache_data.clear()
    st.rerun()
st.sidebar.caption(f"Date de référence des données : {REF_NOW:%d/%m/%Y %H:%M} UTC")

view = pos[pos["depot_id"].isin(sel_depots) & pos["vehicle_type"].isin(sel_types)]

# ------------------------------------------------------------------ en-tête et indicateurs
st.title("Suivi de la flotte")
done, late = nat["a18"]["done"].sum(), nat["a18"]["late"].sum()
s29 = spark_result("S29")
k = st.columns(5)
k[0].metric("Véhicules", f"{len(pos):,}".replace(",", " "))
k[1].metric("En route", int((pos["state"] == "moving").sum()))
k[2].metric("Sans position > 1 h", len(nat["a21"]))
k[3].metric("Missions sur 30 jours", int(nat["a16"]["missions"].sum()))
k[4].metric("Missions en retard", f"{100 * late / done:.1f} %" if done else "-")

tab_fleet, tab_mis, tab_drive, tab_anom, tab_perf = st.tabs(
    ["Flotte", "Missions", "Conduite", "Anomalies (Spark)", "Performances"])

# ------------------------------------------------------------------ flotte
with tab_fleet:
    layers = [pdk.Layer("ScatterplotLayer", data=view, get_position=["lon", "lat"],
                        get_fill_color="color", get_radius=300, radius_min_pixels=3,
                        radius_max_pixels=10, pickable=True)]
    with st.expander("Véhicules dans un rayon autour d'un point (A20, géohash + Haversine)"):
        c = st.columns(3)
        lat = c[0].number_input("Latitude", value=float(view["lat"].iloc[0]) if len(view) else 33.57, format="%.4f")
        lon = c[1].number_input("Longitude", value=float(view["lon"].iloc[0]) if len(view) else -7.59, format="%.4f")
        radius = c[2].number_input("Rayon (km)", value=5.0, min_value=0.5, max_value=50.0)
        found = get_api()[0].a20_within_radius(lat, lon, radius)
        if found:
            hit = pos[pos["plate"].isin([p for _, p in found])]
            layers.append(pdk.Layer("ScatterplotLayer", data=hit, get_position=["lon", "lat"],
                                    get_fill_color=[201, 52, 52], get_radius=600,
                                    radius_min_pixels=8, radius_max_pixels=18, pickable=True))
            st.write(f"{len(found)} véhicule(s) à moins de {radius:g} km : "
                     + ", ".join(f"{p} ({d} km)" for d, p in found[:8]))
        else:
            st.write("Aucun véhicule dans ce rayon (la table `vehicles_by_geohash` a un TTL de 30 min).")
    st.pydeck_chart(pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(latitude=float(view["lat"].mean()) if len(view) else 33.57,
                                         longitude=float(view["lon"].mean()) if len(view) else -7.59, zoom=5.5),
        tooltip={"html": "<b>{plate}</b> · {vehicle_type}<br/>{label}<br/>Vitesse : {speed_kmh} km/h"
                         "<br/>Carburant : {fuel_pct} %<br/>Dernière position : {time}"}), height=520)
    st.caption("Vert : en route · Orange : à l'arrêt · Gris : sans position depuis plus d'une heure.")
    c1, c2 = st.columns(2)
    c1.plotly_chart(px.bar(nat["a19"], x="type", y="avg_kmh", title="Vitesse moyenne par type de véhicule (A19)",
                           labels={"type": "Type", "avg_kmh": "km/h"}), use_container_width=True)
    c2.plotly_chart(px.bar(nat["a23"].head(10), x="avg_fuel", y="model", orientation="h",
                           title="Carburant moyen, 10 modèles les plus bas (A23)",
                           labels={"model": "Modèle", "avg_fuel": "%"}), use_container_width=True)

# ------------------------------------------------------------------ missions
with tab_mis:
    c1, c2 = st.columns(2)
    c1.plotly_chart(px.bar(nat["a16"], x="day", y="missions", title="Missions terminées par jour (A16)",
                           labels={"day": "Jour", "missions": "Missions"}), use_container_width=True)
    c2.plotly_chart(px.line(nat["a16"], x="day", y="km", title="Distance par jour (A16)",
                            labels={"day": "Jour", "km": "km"}), use_container_width=True)
    c3, c4 = st.columns(2)
    c3.plotly_chart(px.bar(nat["a18"], x="depot", y="late_pct", title="Taux de missions en retard par dépôt (A18)",
                           labels={"depot": "Dépôt", "late_pct": "% en retard"}), use_container_width=True)
    c4.plotly_chart(px.bar(nat["a25"], x="depot", y="minutes", title="Durée moyenne des missions par dépôt (A25)",
                           labels={"depot": "Dépôt", "minutes": "Minutes"}), use_container_width=True)

# ------------------------------------------------------------------ conduite
with tab_drive:
    c1, c2 = st.columns(2)
    c1.plotly_chart(px.bar(nat["a17"], x="speeding", y="name", orientation="h",
                           title="Top 10 des excès de vitesse (A17)",
                           labels={"speeding": "Excès de vitesse", "name": "Chauffeur"}).update_yaxes(autorange="reversed"),
                    use_container_width=True)
    if not nat["a22"].empty:
        c2.plotly_chart(px.bar(nat["a22"], x="day", y="count", color="type", barmode="stack",
                               title="Événements de conduite par type et par jour (A22)",
                               labels={"day": "Jour", "count": "Événements", "type": "Type"}), use_container_width=True)
    st.subheader("Score de conduite (S27, Spark)")
    s27 = spark_result("S27")
    if s27 is None:
        need_spark("S27")
    else:
        st.plotly_chart(px.bar(s27.head(15), x="score", y="driver_id", orientation="h",
                               title="15 chauffeurs au score le plus élevé (points pondérés pour 100 km)",
                               labels={"score": "Score", "driver_id": "Chauffeur"}).update_yaxes(autorange="reversed"),
                        use_container_width=True)
        st.dataframe(s27, hide_index=True, use_container_width=True)
        st.caption("Poids : excès de vitesse 3, freinage brusque 2, sortie de zone 2, ralenti 1. "
                   "Distance = missions terminées.")

# ------------------------------------------------------------------ anomalies
with tab_anom:
    s28, s30 = spark_result("S28"), spark_result("S30")
    m = st.columns(4)
    m[0].metric("Arrêts > 30 min (S28)", len(s28) if s28 is not None else "-")
    m[1].metric("Sauts GPS (S29)", len(s29) if s29 is not None else "-")
    m[2].metric("Vols de carburant (S30)", len(s30) if s30 is not None else "-")
    m[3].metric("Entretien > 6 mois (A24)", nat["a24"])
    for title, name, df in (("Arrêts non planifiés pendant une mission (S28)", "S28", s28),
                            ("Anomalies GPS : sauts à plus de 200 km/h (S29)", "S29", s29),
                            ("Baisses de carburant suspectes, moteur éteint (S30)", "S30", s30)):
        st.subheader(title)
        if df is None:
            need_spark(name)
        else:
            st.dataframe(df, hide_index=True, use_container_width=True)
    st.subheader("Véhicules sans position depuis plus d'une heure (A21)")
    st.dataframe(nat["a21"], hide_index=True, use_container_width=True)

# ------------------------------------------------------------------ performances
with tab_perf:
    RES = ROOT / "benchmarks" / "results"
    LABELS = {"query": "Requête", "ms": "Médiane (ms)", "median_ms": "Médiane (ms)", "source": "Famille",
              "mode": "Mode", "niveau": "Niveau de cohérence", "variante": "Méthode"}

    st.subheader("Natif (Cassandra) contre Spark")
    if not COMPARISON.exists():
        st.info("Lancez `python benchmarks/compare.py` pour produire comparison.csv.")
    else:
        cmp_ = pd.read_csv(COMPARISON)
        both = cmp_.dropna(subset=["native_median_ms", "spark_median_ms"])
        if not both.empty:
            long = both.melt(id_vars="query", value_vars=["native_median_ms", "spark_median_ms"],
                             var_name="mode", value_name="ms")
            long["mode"] = long["mode"].map({"native_median_ms": "Natif", "spark_median_ms": "Spark"})
            st.plotly_chart(px.bar(long, x="query", y="ms", color="mode", barmode="group", log_y=True,
                                   title="Médiane par requête (échelle logarithmique)", labels=LABELS),
                            use_container_width=True)
        st.dataframe(cmp_, hide_index=True, use_container_width=True)
        st.caption("Chauffe puis 10 exécutions par requête, même machine et même base. Le natif lit des "
                   "compteurs pré-agrégés à l'écriture ; Spark recalcule à partir des données brutes.")

    st.subheader("Les 30 requêtes")
    all_q = RES / "all_queries.csv"
    if all_q.exists():
        aq = pd.read_csv(all_q)
        meas = aq.dropna(subset=["median_ms"])
        st.plotly_chart(px.bar(meas, x="query", y="median_ms", color="source", log_y=True,
                               title="Médiane de chaque requête (échelle logarithmique)", labels=LABELS),
                        use_container_width=True)
        st.dataframe(aq, hide_index=True, use_container_width=True)
        st.caption("T13 (expiration à 60 jours) est une propriété du schéma (TTL), vérifiée avec une ligne à TTL court "
                   "et non chronométrée. T7 est mesurée en deux temps (T7_start, T7_end).")
    else:
        st.info("Lancez `python benchmarks/compare.py` pour produire all_queries.csv.")

    st.subheader("Effet de la cohérence (ONE, QUORUM, ALL)")
    cons_file = RES / "consistency.csv"
    if cons_file.exists():
        cons = pd.read_csv(cons_file)
        wanted = [q for q in ["A17", "A21", "A24", "T1", "T4", "T7_end"] if q in set(cons["query"])]
        pick = st.multiselect("Requêtes", list(cons["query"]), default=wanted)
        cols = [c for c in cons.columns if c.endswith("_ms")]
        if pick:
            long = cons[cons["query"].isin(pick)].melt(id_vars="query", value_vars=cols,
                                                       var_name="niveau", value_name="ms")
            long["niveau"] = long["niveau"].str.replace("_ms", "", regex=False)
            st.plotly_chart(px.bar(long, x="query", y="ms", color="niveau", barmode="group",
                                   title="Médiane selon le niveau de cohérence", labels=LABELS),
                            use_container_width=True)
        st.caption("Les écarts sont nets sur les balayages de nombreuses partitions (A17, A21, A24) et faibles sur les "
                   "opérations d'une seule partition : les trois nœuds sont sur la même machine, sans latence réseau.")
    st.markdown("**Disponibilité avec des nœuds arrêtés** (RF = 3, test réalisé avec `cqlsh`)")
    st.dataframe(pd.DataFrame(
        {"Situation": ["3 nœuds actifs", "1 nœud arrêté", "2 nœuds arrêtés"],
         "ONE": ["OK", "OK", "OK"],
         "QUORUM": ["OK", "OK", "Indisponible (2 réplicas requis, 1 vivant)"],
         "ALL": ["OK", "Indisponible (3 requis, 2 vivants)", "Indisponible (3 requis, 1 vivant)"]}),
        hide_index=True, use_container_width=True)

    st.subheader("Effet de l'indexation (véhicules d'un dépôt)")
    idx_files = sorted(RES.glob("index_*.csv"))
    if idx_files:
        idx = pd.read_csv(idx_files[-1])
        agg = idx.groupby("query")["ms"].agg(median="median", p95=lambda v: v.quantile(0.95)).reset_index()
        names = {"IDX_scan": "Sans index (balayage)", "IDX_index": "Index secondaire", "IDX_table": "Table dédiée"}
        agg["variante"] = agg["query"].map(names)
        st.plotly_chart(px.bar(agg, x="variante", y="median", title="Médiane (ms), même question, trois méthodes",
                               labels={"median": "Médiane (ms)", "variante": "Méthode"}), use_container_width=True)
        st.caption("Le dépôt DEP01 contient 405 véhicules sur 1 500 : un index secondaire sur une colonne à faible "
                   "cardinalité interroge tous les nœuds et gagne peu ; la table dédiée lit une seule partition.")
    else:
        st.info("Lancez `python benchmarks/index_effect.py --with-drop` pour produire la mesure.")