from pyspark.sql.functions import *

from common.spark_session import get_spark

spark = get_spark()

spark.sql(
    """
    CREATE DATABASE IF NOT EXISTS local.bronze
    """
)

df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka:29092"
    )
    .option(
        "subscribe",
        "order_events"
    )
    .load()
)

bronze_df = df.select(
    col("value").cast("string").alias("raw_value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("ingestion_time")
)

query = (
    bronze_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/home/jovyan/work/checkpoints/customer_bronze"
    )
    .toTable(
        "local.bronze.order_events"
    )
)

query.awaitTermination()