from pyspark.sql.functions import *

from spark.common.spark_session import get_spark

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
        "customer_events"
    )
    .option("startingOffsets","earliest")
    .option("failOnDataLoss","false")
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
        "local.bronze.customer_events"
    )
)

from pyspark.sql.functions import to_json,struct
BRONZE_RAW = "bronze_raw_customer_events"

output_df = (
    bronze_df.select(
        to_json(
            struct("*")
        ).alias("value")
    )
)

output_df1 = (output_df.writeStream.
              format("kafka")
              .option("kafka.bootstrap.servers","kafka:29092")
              .option("topic",BRONZE_RAW)
              .option("checkpointLocation",
                      "/home/jovyan/work/checkpoints/customer_events_raw")
              .start()        
                      )


spark.streams.awaitAnyTermination()