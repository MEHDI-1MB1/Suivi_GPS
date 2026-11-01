"""Jobs Spark S26 à S30 sur Cassandra (keyspace fleet).

Lancement (cluster) :
  spark-submit --master spark://spark-master:7077 \
     --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
     /opt/spark-jobs/s26_s30_cassandra.py [S26 S27 ...]

Chaque job écrit ses lignes dans OUT_DIR/<job>.json (lu ensuite par validate.py).
"""
import json
import os
import sys
import time

import pyspark.sql.functions as F
from pyspark.sql import Window

from spark_common import add_distance, get_spark, read_table

OUT_DIR = os.getenv("OUT_DIR", "/opt/spark-jobs/out")

# Poids du score de conduite (S27) : à justifier dans le rapport
WEIGHTS = {"SPEEDING": 3, "HARSH_BRAKING": 2, "LONG_IDLE": 1, "GEOFENCE_EXIT": 2}


def positions(spark):
    return read_table(spark, "positions_by_vehicle_day")


def s26(spark):
    """Kilométrage réel par véhicule et par jour (Haversine entre positions consécutives)."""
    d = add_distance(positions(spark), key="plate")
    ok = F.col("implied_kmh").isNull() | (F.col("implied_kmh") <= 200)   # sauts GPS exclus
    return (d.filter(ok).groupBy("plate", "day")
             .agg(F.round(F.sum("dist_km"), 2).alias("km"))
             .orderBy("plate", "day"))


def s27(spark):
    """Score de conduite : points pondérés pour 100 km (km = missions terminées), puis rang."""
    weight = F.lit(0)
    for etype, w in WEIGHTS.items():
        weight = F.when(F.col("event_type") == etype, w).otherwise(weight)
    pts = (read_table(spark, "events_by_driver_day").withColumn("w", weight)
           .groupBy("driver_id")
           .agg(F.sum("w").alias("points"), F.count("*").alias("n_events")))
    km = (read_table(spark, "missions").filter("status = 'DONE'")
          .groupBy("driver_id").agg(F.sum("distance_km").alias("km")))
    s = (pts.join(km, "driver_id").filter("km > 0")
         .withColumn("score", F.round(F.col("points") / (F.col("km") / 100), 2))
         .withColumn("km", F.round("km", 1)))
    return s.withColumn("rank", F.rank().over(Window.orderBy(F.desc("score")))).orderBy("rank")


def s28(spark):
    """Arrêts de plus de 30 min pendant une mission (jointure missions x positions)."""
    pos = positions(spark).select("plate", "ts", "speed_kmh")
    m = (read_table(spark, "missions").filter("actual_start IS NOT NULL")
         .select("mission_id", F.col("plate").alias("m_plate"),
                 F.col("actual_start").alias("m_start"),
                 F.coalesce("actual_end", "planned_end").alias("m_end")))
    j = pos.join(m, (pos.plate == m.m_plate) & pos.ts.between(m.m_start, m.m_end))

    w = Window.partitionBy("mission_id").orderBy("ts")
    wcum = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    runs = (j.withColumn("stopped", (F.col("speed_kmh") < 1).cast("int"))
             .withColumn("chg", (F.col("stopped") != F.lag("stopped").over(w)).cast("int"))
             .withColumn("run_id", F.sum(F.coalesce("chg", F.lit(0))).over(wcum)))
    return (runs.filter("stopped = 1")
            .groupBy("mission_id", "plate", "run_id")
            .agg(F.min("ts").alias("stop_start"), F.max("ts").alias("stop_end"))
            .withColumn("duration_min",
                        F.round((F.col("stop_end").cast("long")
                                 - F.col("stop_start").cast("long")) / 60, 1))
            .filter(F.col("duration_min") > 30)
            .orderBy("stop_start"))


def s29(spark):
    """Anomalies GPS : sauts de position impliquant plus de 200 km/h.

    Un saut « aller-retour » (la position s'écarte puis revient) produit deux segments
    consécutifs au-dessus du seuil : on les regroupe en un seul événement."""
    d = add_distance(positions(spark), key="plate")
    w = Window.partitionBy("plate", "day").orderBy("ts")
    d = (d.withColumn("bad", F.coalesce(F.col("implied_kmh") > 200, F.lit(False)))
          .withColumn("prev_bad", F.coalesce(F.lag("bad").over(w), F.lit(False))))
    return (d.filter(F.col("bad") & ~F.col("prev_bad"))
             .select("plate", "prev_ts", "ts",
                     F.round("dist_km", 1).alias("dist_km"),
                     F.round("implied_kmh", 0).alias("kmh"))
             .orderBy("ts"))


def s30(spark):
    """Vol de carburant suspect : baisse de plus de 20 points en 10 min, moteur éteint."""
    pos = (positions(spark).select("plate", "ts", "fuel_pct", "engine_state")
           .withColumn("on", F.when(F.col("engine_state") == "OFF", 0).otherwise(1)))
    w10 = (Window.partitionBy("plate").orderBy(F.col("ts").cast("long"))
           .rangeBetween(-600, 0))                                  # 10 minutes glissantes
    r = (pos.withColumn("fuel_max", F.max("fuel_pct").over(w10))
            .withColumn("on_max", F.max("on").over(w10))            # 0 = moteur éteint sur toute la fenêtre
            .withColumn("drop", F.col("fuel_max") - F.col("fuel_pct"))
            .filter((F.col("on_max") == 0) & (F.col("drop") > 20)))
    return (r.groupBy("plate")
             .agg(F.min("ts").alias("first_alert"), F.round(F.max("drop"), 1).alias("max_drop")))


JOBS = {"S26": s26, "S27": s27, "S28": s28, "S29": s29, "S30": s30}


def main():
    names = [a.upper() for a in sys.argv[1:]] or list(JOBS)
    spark = get_spark("s26-s30-cassandra")
    os.makedirs(OUT_DIR, exist_ok=True)
    for name in names:
        t0 = time.perf_counter()
        rows = [r.asDict() for r in JOBS[name](spark).collect()]
        ms = (time.perf_counter() - t0) * 1000
        with open(os.path.join(OUT_DIR, f"{name}.json"), "w") as f:
            json.dump(rows, f, default=str, indent=1)
        print(f"{name}: {len(rows):>6} lignes en {ms:8.0f} ms")
        for r in rows[:3]:
            print("     ", r)
    spark.stop()


if __name__ == "__main__":
    main()