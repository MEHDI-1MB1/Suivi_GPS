import os
import pyspark.sql.functions as F
from spark_common import get_spark, add_distance, DATA_DIR
from benchmark import benchmark

spark = get_spark("bench-s26")


def s26():
    df = (spark.read.csv(f"{DATA_DIR}/sample_positions.csv", header=True, inferSchema=True)
          .withColumn("ts", F.to_timestamp("ts"))
          .withColumn("day", F.to_date("ts")))
    return (add_distance(df).groupBy("vehicle_id", "day")
            .agg(F.sum("dist_km").alias("km")).collect())   # collect() = action obligatoire


benchmark("S26", s26, mode=os.getenv("MODE", "spark_local"), runs=10)
spark.stop()