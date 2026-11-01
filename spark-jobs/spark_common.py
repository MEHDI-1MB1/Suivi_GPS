from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import os
DATA_DIR = os.getenv("DATA_DIR", "data/csv")

R = 6371.0088  # rayon moyen de la Terre (km)


def get_spark(name):
    spark = (SparkSession.builder.appName(name)
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def add_distance(df):
    """Ajoute dist_km et implied_kmh entre positions consécutives (par véhicule et jour)."""
    w = Window.partitionBy("vehicle_id", "day").orderBy("ts")
    df = (df.withColumn("prev_lat", F.lag("lat").over(w))
            .withColumn("prev_lon", F.lag("lon").over(w))
            .withColumn("prev_ts", F.lag("ts").over(w)))
    dlat = F.radians(F.col("lat") - F.col("prev_lat"))
    dlon = F.radians(F.col("lon") - F.col("prev_lon"))
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians("prev_lat")) * F.cos(F.radians("lat")) * F.sin(dlon / 2) ** 2)
    df = df.withColumn("dist_km", 2 * R * F.asin(F.sqrt(a)))
    dt_h = (F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 3600.0
    return df.withColumn("implied_kmh", F.col("dist_km") / dt_h)