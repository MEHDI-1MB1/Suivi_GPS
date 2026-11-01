"""Tableau de bord de la flotte (Streamlit).

Toutes les données passent par load_data() : pour brancher Cassandra plus tard,
il suffira de remplacer cette fonction (mêmes colonnes en sortie).
Lancer :  streamlit run dashboard/app.py
"""
import os
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st

DATA_DIR = Path(os.getenv("DATA_DIR", "data/csv"))
BENCH_FILE = Path(os.getenv("RESULTS_FILE", "results/benchmarks.csv"))
R_KM = 6371.0088
# Poids du score de conduite (S27) : à justifier dans le rapport
WEIGHTS = {"speeding": 3, "harsh_braking": 2, "idle": 1, "zone_exit": 2}
TYPE_LABELS = {"speeding": "Excès de vitesse", "harsh_braking": "Freinage brusque",
               "idle": "Ralenti prolongé", "zone_exit": "Sortie de zone"}
COLORS = {"ok": [31, 157, 138], "stopped": [224, 122, 40], "alert": [201, 52, 52]}

st.set_page_config(page_title="Flotte : suivi en direct", page_icon="🚚", layout="wide")


# ---------------------------------------------------------------- données
@st.cache_data
def load_data():
    pos = pd.read_csv(DATA_DIR / "sample_positions.csv", parse_dates=["ts"])
    ev = pd.read_csv(DATA_DIR / "sample_events.csv", parse_dates=["ts"])
    ms = pd.read_csv(DATA_DIR / "sample_missions.csv", parse_dates=["start_ts", "end_ts"])
    # Heures en UTC, sans fuseau attaché (comparaisons et curseur de période plus simples)
    for df, cols in ((pos, ["ts"]), (ev, ["ts"]), (ms, ["start_ts", "end_ts"])):
        for c in cols:
            df[c] = df[c].dt.tz_localize(None)
    return pos, ev, ms


def haversine(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = (np.sin((p2 - p1) / 2) ** 2
         + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lon2 - lon1) / 2) ** 2)
    return 2 * R_KM * np.arcsin(np.sqrt(a))


@st.cache_data
def enrich(pos):
    """Distance et vitesse implicite entre positions consécutives (S26 / S29)."""
    pos = pos.sort_values(["vehicle_id", "ts"]).reset_index(drop=True)
    g = pos.groupby("vehicle_id")
    prev_lat, prev_lon, prev_ts = g["lat"].shift(), g["lon"].shift(), g["ts"].shift()
    pos["dist_km"] = haversine(prev_lat, prev_lon, pos["lat"], pos["lon"])
    hours = (pos["ts"] - prev_ts).dt.total_seconds() / 3600
    pos["kmh"] = pos["dist_km"] / hours
    pos["jump"] = pos["kmh"] > 200
    return pos


def find_stops(pos, min_minutes=30):
    """Arrêts de plus de 30 min (S28), vitesse < 1 km/h."""
    out = []
    for vid, d in pos.groupby("vehicle_id"):
        d = d.sort_values("ts")
        stopped = d["speed"] < 1
        run = (stopped != stopped.shift()).cumsum()
        for _, r in d[stopped].groupby(run[stopped]):
            minutes = (r["ts"].max() - r["ts"].min()).total_seconds() / 60
            if minutes > min_minutes:
                out.append({"Véhicule": vid, "Début": r["ts"].min(),
                            "Fin": r["ts"].max(), "Durée (min)": round(minutes)})
    return pd.DataFrame(out, columns=["Véhicule", "Début", "Fin", "Durée (min)"])


def find_fuel_drops(pos, threshold=20):
    """Baisse de carburant > 20 points en 10 min, moteur éteint (S30)."""
    out = []
    for vid, d in pos.groupby("vehicle_id"):
        d = d.sort_values("ts").set_index("ts")
        fuel_max = d["fuel_level"].rolling("10min").max()
        eng_max = d["engine_on"].rolling("10min").max()
        drop = fuel_max - d["fuel_level"]
        hit = (eng_max == 0) & (drop > threshold)
        if hit.any():
            out.append({"Véhicule": vid, "Première alerte": hit[hit].index.min(),
                        "Baisse max (points)": round(drop[hit].max(), 1)})
    return pd.DataFrame(out, columns=["Véhicule", "Première alerte", "Baisse max (points)"])


def driving_score(pos, ev, ms):
    """Score de conduite par chauffeur : points pondérés pour 100 km (S27)."""
    km_by_vehicle = pos.loc[~pos["jump"].fillna(False)].groupby("vehicle_id")["dist_km"].sum()
    driver_of = ms.drop_duplicates("vehicle_id").set_index("vehicle_id")["driver_id"]
    km = km_by_vehicle.rename(index=driver_of).groupby(level=0).sum().rename("km")
    pts = (ev.assign(points=ev["type"].map(WEIGHTS).fillna(0))
             .groupby("driver_id").agg(points=("points", "sum"), evenements=("type", "count")))
    s = pts.join(km, how="inner")
    s["score"] = (s["points"] / (s["km"] / 100)).round(2)
    s["km"] = s["km"].round(1)
    s = s.sort_values("score", ascending=False).reset_index()
    s.insert(0, "rang", range(1, len(s) + 1))
    return s


# ---------------------------------------------------------------- chargement
try:
    pos_raw, ev, ms = load_data()
except FileNotFoundError as e:
    st.error(f"Fichier introuvable : {e.filename}. "
             "Lancez d'abord `python src/make_sample.py` à la racine du projet.")
    st.stop()

pos = enrich(pos_raw)

# ---------------------------------------------------------------- filtres
st.sidebar.header("Filtres")
vehicles = sorted(pos["vehicle_id"].unique())
sel_vehicles = st.sidebar.multiselect("Véhicules", vehicles, default=vehicles)
tmin, tmax = pos["ts"].min().to_pydatetime(), pos["ts"].max().to_pydatetime()
t_from, t_to = st.sidebar.slider("Période (UTC)", tmin, tmax, (tmin, tmax), format="HH:mm")
focus = st.sidebar.selectbox("Véhicule à suivre sur la carte", ["Aucun"] + vehicles)
st.sidebar.caption("Source : fichiers CSV d'exemple. Cassandra sera branchée via load_data().")

mask = (pos["vehicle_id"].isin(sel_vehicles)
        & (pos["ts"] >= pd.Timestamp(t_from)) & (pos["ts"] <= pd.Timestamp(t_to)))
view = pos[mask]
ev_view = ev[ev["vehicle_id"].isin(sel_vehicles)
             & (ev["ts"] >= pd.Timestamp(t_from)) & (ev["ts"] <= pd.Timestamp(t_to))]

if view.empty:
    st.warning("Aucune position pour ces filtres. Élargissez la période ou choisissez un véhicule.")
    st.stop()

stops = find_stops(view)
fuel_alerts = find_fuel_drops(view)
jumps = view[view["jump"]]

# ---------------------------------------------------------------- en-tête + KPI
st.title("Suivi de la flotte")
st.caption(f"{len(view):,} positions · {len(sel_vehicles)} véhicules · "
           f"{t_from:%d/%m %H:%M} à {t_to:%H:%M} UTC".replace(",", " "))

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Distance parcourue", f"{view.loc[~view['jump'].fillna(False), 'dist_km'].sum():.0f} km")
k2.metric("Vitesse moyenne", f"{view.loc[view['speed'] > 0, 'speed'].mean():.0f} km/h")
k3.metric("Événements de conduite", len(ev_view))
k4.metric("Anomalies GPS", len(jumps))
k5.metric("Alertes carburant", len(fuel_alerts))

tab_map, tab_drive, tab_alerts, tab_perf = st.tabs(
    ["Carte", "Conduite", "Anomalies", "Performances"])

# ---------------------------------------------------------------- carte
with tab_map:
    last = view.sort_values("ts").groupby("vehicle_id").tail(1).copy()
    alert_ids = set(fuel_alerts["Véhicule"]) | set(jumps["vehicle_id"])
    last["state"] = np.where(last["vehicle_id"].isin(alert_ids), "alert",
                             np.where(last["speed"] < 1, "stopped", "ok"))
    last["color"] = last["state"].map(COLORS)
    last["label"] = last["state"].map({"ok": "En route", "stopped": "À l'arrêt", "alert": "Alerte"})
    last["time"] = last["ts"].dt.strftime("%H:%M:%S")

    layers = []
    if focus != "Aucun":
        trail = view[view["vehicle_id"] == focus].sort_values("ts")
        layers.append(pdk.Layer(
            "PathLayer", data=pd.DataFrame({"path": [trail[["lon", "lat"]].values.tolist()]}),
            get_path="path", get_color=[60, 90, 140], width_min_pixels=3))
    layers.append(pdk.Layer(
        "ScatterplotLayer", data=last, get_position=["lon", "lat"], get_fill_color="color",
        get_line_color=[255, 255, 255], line_width_min_pixels=2,
        get_radius=450, radius_min_pixels=7, radius_max_pixels=18, pickable=True))

    st.pydeck_chart(pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(latitude=float(last["lat"].mean()),
                                         longitude=float(last["lon"].mean()), zoom=9),
        tooltip={"html": "<b>Véhicule {vehicle_id}</b><br/>{label}<br/>"
                         "Vitesse : {speed} km/h<br/>Carburant : {fuel_level} %<br/>"
                         "Dernière position : {time}"}), height=520)
    st.caption("Vert : en route · Orange : à l'arrêt · Rouge : alerte (saut GPS ou carburant). "
               "Choisissez un véhicule dans la barre latérale pour afficher son trajet.")

    c1, c2 = st.columns(2)
    with c1:
        fig = px.line(view, x="ts", y="speed", color=view["vehicle_id"].astype(str),
                      labels={"ts": "Heure", "speed": "Vitesse (km/h)", "color": "Véhicule"},
                      title="Vitesse au cours du temps")
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        fig = px.line(view, x="ts", y="fuel_level", color=view["vehicle_id"].astype(str),
                      labels={"ts": "Heure", "fuel_level": "Carburant (%)", "color": "Véhicule"},
                      title="Niveau de carburant")
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------- conduite
with tab_drive:
    c1, c2 = st.columns(2)
    with c1:
        if ev_view.empty:
            st.info("Aucun événement de conduite sur cette sélection.")
        else:
            by_type = (ev_view.assign(Type=ev_view["type"].map(TYPE_LABELS))
                       .groupby(["driver_id", "Type"]).size().reset_index(name="Nombre"))
            by_type["driver_id"] = by_type["driver_id"].astype(str)
            fig = px.bar(by_type, x="driver_id", y="Nombre", color="Type", barmode="stack",
                         labels={"driver_id": "Chauffeur"},
                         title="Événements par chauffeur et par type")
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        score = driving_score(view, ev_view, ms)
        if score.empty:
            st.info("Pas assez de données pour calculer un score.")
        else:
            fig = px.bar(score, x="score", y=score["driver_id"].astype(str), orientation="h",
                         labels={"score": "Points pour 100 km", "y": "Chauffeur"},
                         title="Score de conduite (plus c'est haut, plus c'est risqué)")
            fig.update_yaxes(autorange="reversed")
            st.plotly_chart(fig, use_container_width=True)
    if not ev_view.empty:
        st.subheader("Classement détaillé")
        st.dataframe(score.rename(columns={"rang": "Rang", "driver_id": "Chauffeur",
                                           "points": "Points", "evenements": "Événements",
                                           "km": "Distance (km)", "score": "Score"}),
                     hide_index=True, use_container_width=True)
        st.caption("Poids : excès de vitesse 3, freinage brusque 2, sortie de zone 2, "
                   "ralenti 1. Les sauts GPS sont exclus de la distance.")

# ---------------------------------------------------------------- anomalies
with tab_alerts:
    a, b, c = st.columns(3)
    a.metric("Arrêts de plus de 30 min", len(stops))
    b.metric("Sauts GPS (> 200 km/h)", len(jumps))
    c.metric("Baisses de carburant suspectes", len(fuel_alerts))

    st.subheader("Arrêts non planifiés")
    if stops.empty:
        st.success("Aucun arrêt de plus de 30 minutes.")
    else:
        st.dataframe(stops, hide_index=True, use_container_width=True)

    st.subheader("Anomalies GPS")
    if jumps.empty:
        st.success("Aucun saut de position détecté.")
    else:
        st.dataframe(jumps[["vehicle_id", "ts", "dist_km", "kmh"]].round(1)
                     .rename(columns={"vehicle_id": "Véhicule", "ts": "Heure",
                                      "dist_km": "Distance (km)", "kmh": "Vitesse implicite (km/h)"}),
                     hide_index=True, use_container_width=True)

    st.subheader("Carburant : baisses suspectes")
    if fuel_alerts.empty:
        st.success("Aucune baisse suspecte moteur éteint.")
    else:
        st.dataframe(fuel_alerts, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------- performances
with tab_perf:
    st.subheader("Temps d'exécution des requêtes")
    if not BENCH_FILE.exists():
        st.info("Aucun benchmark trouvé. Lancez `python src/bench_spark_sample.py` "
                f"pour produire {BENCH_FILE}.")
    else:
        bench = pd.read_csv(BENCH_FILE)
        bench["variante"] = bench["mode"] + bench["consistency"].fillna("").map(
            lambda c: f" ({c})" if c else "")
        agg = (bench.groupby(["query_id", "variante"])["duration_ms"]
               .agg(médiane="median", moyenne="mean", p95=lambda s: s.quantile(0.95), mesures="count")
               .round(1).reset_index())
        fig = px.bar(agg, x="query_id", y="médiane", color="variante", barmode="group",
                     labels={"query_id": "Requête", "médiane": "Médiane (ms)", "variante": "Mode"},
                     title="Médiane par requête et par mode")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(agg, hide_index=True, use_container_width=True)
        st.caption("Chaque mesure = 1 exécution de chauffe puis 10 exécutions. "
                   "Comparez natif vs Spark et ONE / QUORUM / ALL ici.")