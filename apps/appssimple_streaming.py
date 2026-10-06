from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = (
    SparkSession.builder
    .appName("simple-rate-streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20)
    .option("numPartitions", 2)
    .load()
)

transformed = (
    stream
    .withColumn("category", expr("CASE WHEN value % 2 = 0 THEN 'even' ELSE 'odd' END"))
    .withColumn("value_x10", col("value") * 20)
)

query = (
    transformed.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 10)
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()