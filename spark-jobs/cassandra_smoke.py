import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("cassandra-smoke")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.cassandra.connection.host", os.getenv("CASSANDRA_HOST", "cassandra1"))
         .config("spark.cassandra.connection.port", "9042")
         .config("spark.cassandra.connection.localDC", "dc1")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def table(name):
    return (spark.read.format("org.apache.spark.sql.cassandra")
            .options(keyspace="fleet", table=name).load())


expected = {"drivers": 2000, "vehicles": 1500, "positions_by_vehicle_day": 120069,
            "missions": 12000, "events_by_driver_day": 16000,
            "maintenances_by_vehicle": 2551}
for name, n in expected.items():
    c = table(name).count()
    print(f"{name:28s} {c:>8}  (attendu {n})  {'OK' if c == n else 'ECART'}")

pos = table("positions_by_vehicle_day")
pos.printSchema()
pos.show(3, truncate=False)
spark.stop()
