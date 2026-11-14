"""Mesure les jobs Spark (chauffe + 10 exécutions chacun) et écrit un CSV au format de A.

Lancement sur le cluster :
  docker exec -e PYTHONPATH=/opt/spark-jobs:/opt/benchmarks \
     -e RESULTS_DIR=/opt/spark-jobs/out -e BENCH_PREFIX=spark_bench \
     spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
     --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
     --conf spark.jars.ivy=/tmp/.ivy --conf spark.executor.memory=512m \
     /opt/spark-jobs/bench_spark.py [S26 A17 ...]
"""
import os
import sys

import pyspark.sql.functions as F

import s26_s30_cassandra as jobs
from benchmark import benchmark
from spark_common import get_spark, read_table

MODE = os.getenv("BENCH_MODE", "spark_cluster")


def a17_spark(spark):
    """A17 version Spark : top 10 des chauffeurs par excès de vitesse, avec leur nom.
    Égalités départagées par driver_id décroissant, comme le natif (heapq.nlargest)."""
    top = (read_table(spark, "events_by_driver_day").filter("event_type = 'SPEEDING'")
           .groupBy("driver_id").agg(F.count("*").alias("speeding"))
           .orderBy(F.desc("speeding"), F.desc("driver_id")).limit(10))
    names = read_table(spark, "drivers").select("driver_id", "name")
    return (top.join(F.broadcast(names), "driver_id", "left")
            .orderBy(F.desc("speeding"), F.desc("driver_id")))


def a19_spark(spark):
    """A19 version Spark : vitesse moyenne par type de véhicule (tous les points GPS,
    arrêts compris, comme le natif)."""
    pos = read_table(spark, "positions_by_vehicle_day").select("plate", "speed_kmh")
    veh = read_table(spark, "vehicles").select("plate", "vehicle_type")
    return (pos.join(F.broadcast(veh), "plate").groupBy("vehicle_type")
            .agg(F.round(F.avg("speed_kmh"), 1).alias("avg_kmh"), F.count("*").alias("samples"))
            .orderBy(F.desc("avg_kmh")))


REF_DATE = os.getenv("REF_DATE", "2026-10-31")   # date de référence du chargement (--now)


def a16_spark(spark):
    """A16 version Spark : missions terminées et distance par jour sur les 30 derniers jours.
    Même règle que le chargeur de A : statut DONE, jour = date de actual_start,
    distance_m = int(distance_km * 1000)."""
    ref = F.lit(REF_DATE).cast("date")
    m = (read_table(spark, "missions").filter("status = 'DONE'")
         .withColumn("day", F.to_date("actual_start")))
    return (m.filter(F.col("day").between(F.date_sub(ref, 29), ref))
            .groupBy("day")
            .agg(F.count("*").alias("missions"),
                 F.round(F.sum((F.col("distance_km") * 1000).cast("long")) / 1000, 1).alias("km"))
            .orderBy("day"))


def a18_spark(spark):
    """A18 version Spark : taux de missions en retard par dépôt (actual_end > planned_end)."""
    m = read_table(spark, "missions").filter("status = 'DONE'")
    return (m.groupBy("depot_id")
            .agg(F.count("*").alias("done"),
                 F.sum(F.when(F.col("actual_end") > F.col("planned_end"), 1).otherwise(0)).alias("late"))
            .withColumn("late_pct", F.round(100.0 * F.col("late") / F.col("done"), 1))
            .orderBy(F.desc("late_pct")))


QUERIES = {"S26": jobs.s26, "S27": jobs.s27, "S28": jobs.s28, "S29": jobs.s29,
           "S30": jobs.s30, "A16": a16_spark, "A17": a17_spark, "A18": a18_spark,
           "A19": a19_spark}


def main():
    names = [a.upper() for a in sys.argv[1:]] or list(QUERIES)
    spark = get_spark("bench-spark")
    for name in names:
        fn = QUERIES[name]
        if os.getenv("SHOW"):                      # SHOW=1 : affiche le résultat, ne mesure pas
            print(f"--- {name}")
            df = fn(spark)
            for r in df.limit(10).collect():
                print("   ", r.asDict())
            if "missions" in df.columns:               # A16 : totaux à comparer au natif
                print("    total        :", df.agg(F.sum("missions").alias("missions"),
                                                    F.round(F.sum("km"), 1).alias("km")).first().asDict())
                print("    dernier jour :", df.orderBy(F.desc("day")).first().asDict())
        else:
            benchmark(name, lambda: fn(spark).collect(), mode=MODE, warmup=2)
    spark.stop()


if __name__ == "__main__":
    main()