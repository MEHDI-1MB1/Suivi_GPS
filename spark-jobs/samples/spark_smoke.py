from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("smoke")
    .master("local[*]")
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1")
    .config("spark.cassandra.connection.host", "localhost")
    .getOrCreate())

df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="t", keyspace="test").load()
df.show()