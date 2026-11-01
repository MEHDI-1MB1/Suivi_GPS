from pyspark.sql import Window
import pyspark.sql.functions as F
from spark_common import get_spark, add_distance, DATA_DIR

spark = get_spark("s27-s28-s30")

pos = (spark.read.csv(f"{DATA_DIR}/sample_positions.csv", header=True, inferSchema=True)
       .withColumn("ts", F.to_timestamp("ts"))
       .withColumn("day", F.to_date("ts")))
missions = (spark.read.csv(f"{DATA_DIR}/sample_missions.csv", header=True, inferSchema=True)
            .withColumn("start_ts", F.to_timestamp("start_ts"))
            .withColumn("end_ts", F.to_timestamp("end_ts"))
            .select("mission_id", F.col("vehicle_id").alias("m_vid"),
                    "driver_id", "start_ts", "end_ts"))
events = spark.read.csv(f"{DATA_DIR}/sample_events.csv", header=True, inferSchema=True)

posd = add_distance(pos)

# Positions rattachées à leur mission (jointure sur véhicule + intervalle de temps)
j = posd.join(missions, (posd.vehicle_id == missions.m_vid)
                        & posd.ts.between(missions.start_ts, missions.end_ts))

# ---------- S28 : arrêts > 30 min pendant une mission ----------
wm = Window.partitionBy("mission_id").orderBy("ts")
wcum = wm.rowsBetween(Window.unboundedPreceding, Window.currentRow)
runs = (j.withColumn("stopped", (F.col("speed") < 1).cast("int"))
         .withColumn("chg", (F.col("stopped") != F.lag("stopped").over(wm)).cast("int"))
         .withColumn("run_id", F.sum(F.coalesce(F.col("chg"), F.lit(0))).over(wcum)))
s28 = (runs.filter("stopped = 1")
       .groupBy("mission_id", "vehicle_id", "run_id")
       .agg(F.min("ts").alias("stop_start"), F.max("ts").alias("stop_end"))
       .withColumn("duration_min",
                   (F.col("stop_end").cast("long") - F.col("stop_start").cast("long")) / 60)
       .filter(F.col("duration_min") > 30))
print("=== S28 : arrêts > 30 min en mission ===")
s28.show(truncate=False)

# ---------- S30 : vol de carburant suspect ----------
w10 = (Window.partitionBy("vehicle_id")
       .orderBy(F.col("ts").cast("long")).rangeBetween(-600, 0))   # 10 minutes glissantes
s30 = (pos.withColumn("fuel_max", F.max("fuel_level").over(w10))
          .withColumn("eng_max", F.max("engine_on").over(w10))
          .withColumn("drop", F.col("fuel_max") - F.col("fuel_level"))
          .filter((F.col("eng_max") == 0) & (F.col("drop") > 20))
          .groupBy("vehicle_id")
          .agg(F.min("ts").alias("first_alert"), F.round(F.max("drop"), 1).alias("max_drop")))
print("=== S30 : baisse > 20 points en 10 min, moteur éteint ===")
s30.show(truncate=False)

# ---------- S27 : score de conduite par 100 km ----------
weight = (F.when(F.col("type") == "speeding", 3)
           .when(F.col("type") == "harsh_braking", 2)
           .when(F.col("type") == "idle", 1)
           .when(F.col("type") == "zone_exit", 2)
           .otherwise(0))
pts = (events.withColumn("w", weight).groupBy("driver_id")
       .agg(F.sum("w").alias("points"), F.count("*").alias("n_events")))
km = (j.filter(F.col("implied_kmh").isNull() | (F.col("implied_kmh") <= 200))  # exclut les sauts GPS
       .groupBy("driver_id").agg(F.sum("dist_km").alias("km")))
score = (pts.join(km, "driver_id")
         .withColumn("score", F.round(F.col("points") / (F.col("km") / 100), 2))
         .withColumn("km", F.round("km", 1))
         .withColumn("rank", F.rank().over(Window.orderBy(F.desc("score"))))
         .orderBy("rank"))
print("=== S27 : score de conduite (points / 100 km) ===")
score.show()

spark.stop()