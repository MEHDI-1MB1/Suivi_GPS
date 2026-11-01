"""Outils communs aux jobs Spark : session, lecture Cassandra, Haversine.

Remplace l'ancien spark_common.py. Les jobs d'échantillon (clé "vehicle_id")
continuent de fonctionner : add_distance(df) garde ce comportement par défaut.
"""
import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window

R = 6371.0088  # rayon moyen de la Terre (km)
DATA_DIR = os.getenv("DATA_DIR", "data/csv")


def get_spark(name):
    spark = (SparkSession.builder.appName(name)
             .config("spark.sql.session.timeZone", "UTC")
             # peu de données : 200 partitions de shuffle (valeur par défaut) ralentiraient tout
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.cassandra.connection.host", os.getenv("CASSANDRA_HOST", "cassandra1"))
             .config("spark.cassandra.connection.port", os.getenv("CASSANDRA_PORT", "9042"))
             .config("spark.cassandra.connection.localDC", "dc1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_table(spark, name, keyspace="fleet"):
    return (spark.read.format("org.apache.spark.sql.cassandra")
            .options(keyspace=keyspace, table=name).load())


def add_distance(df, key="vehicle_id"):
    """Ajoute dist_km et implied_kmh entre positions consécutives.

    Colonnes attendues : <key>, day, ts, lat, lon.
    Fenêtre : par véhicule et par jour (limite : un segment à cheval sur minuit est perdu).
    """
    w = Window.partitionBy(key, "day").orderBy("ts")
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