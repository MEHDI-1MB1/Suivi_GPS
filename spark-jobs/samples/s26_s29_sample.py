from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

R = 6371.0088  # rayon moyen de la Terre en km

spark = (SparkSession.builder.appName("haversine-sample")
         .master("local[*]")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = (spark.read.csv("data/csv/sample_positions.csv", header=True, inferSchema=True)
      .withColumn("ts", F.to_timestamp("ts"))
      .withColumn("day", F.to_date("ts")))

# Position précédente, par véhicule et par jour, dans l'ordre du temps
w = Window.partitionBy("vehicle_id", "day").orderBy("ts")
df = (df.withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("prev_ts", F.lag("ts").over(w)))

# Formule de Haversine avec des fonctions Spark natives (pas d'UDF)
dlat = F.radians(F.col("lat") - F.col("prev_lat"))
dlon = F.radians(F.col("lon") - F.col("prev_lon"))
a = (F.sin(dlat / 2) ** 2
     + F.cos(F.radians("prev_lat")) * F.cos(F.radians("lat")) * F.sin(dlon / 2) ** 2)
df = df.withColumn("dist_km", 2 * R * F.asin(F.sqrt(a)))

# Vitesse implicite entre deux positions
dt_h = (F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 3600.0
df = df.withColumn("implied_kmh", F.col("dist_km") / dt_h)

# S26 : kilométrage par véhicule et par jour
print("=== S26 : km par véhicule et par jour ===")
(df.groupBy("vehicle_id", "day")
   .agg(F.round(F.sum("dist_km"), 2).alias("km"))
   .orderBy("vehicle_id", "day").show())

# S29 : sauts de position impliquant plus de 200 km/h
print("=== S29 : anomalies (> 200 km/h) ===")
(df.filter(F.col("implied_kmh") > 200)
   .select("vehicle_id", "prev_ts", "ts", F.round("dist_km", 1).alias("dist_km"),
           F.round("implied_kmh", 0).alias("kmh")).show())

spark.stop()