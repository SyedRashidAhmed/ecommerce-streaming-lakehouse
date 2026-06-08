from pyspark.sql.functions import *
from pyspark.sql.types import *

from spark.common.spark_session import get_spark

spark = get_spark()

# ==================================================
# DATABASE
# ==================================================

spark.sql("""
CREATE DATABASE IF NOT EXISTS local.silver
""")

# ==================================================
# SILVER TABLE
# ==================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS local.silver.order_events (

    event_id STRING,
    event_type STRING,
    event_time TIMESTAMP,

    order_id STRING,
    customer_id STRING,
    product_id STRING,

    quantity INT,
    unit_price DOUBLE,
    total_amount DOUBLE,

    source_topic STRING,
    source_partition INT,
    source_offset BIGINT,

    kafka_timestamp TIMESTAMP,
    bronze_ingestion_time TIMESTAMP,

    event_date DATE

)
USING ICEBERG
PARTITIONED BY (event_date)
""")

# ==================================================
# DLQ TABLE
# ==================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS local.silver.order_events_dlq (

    raw_value STRING,

    error_type STRING,
    error_reason STRING,

    source_topic STRING,
    source_partition INT,
    source_offset BIGINT,

    kafka_timestamp TIMESTAMP,
    bronze_ingestion_time TIMESTAMP

)
USING ICEBERG
""")

# ==================================================
# ORDER SCHEMA
# ==================================================

order_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("total_amount", DoubleType())
])

# ==================================================
# READ BRONZE RAW TOPIC
# ==================================================

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "bronze_raw_order_events")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# ==================================================
# PARSE BRONZE PAYLOAD
# ==================================================

bronze_schema = StructType([
    StructField("raw_value", StringType()),
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("kafka_timestamp", StringType()),
    StructField("ingestion_time", StringType())
])

bronze_df = (
    df.selectExpr("CAST(value AS STRING) as value")
)

parsed_df = (
    bronze_df.withColumn(
        "bronze",
        from_json(col("value"), bronze_schema)
    )
)

flattened_df = parsed_df.select("bronze.*")

# ==================================================
# PARSE ORDER JSON
# ==================================================

order_df = (
    flattened_df.withColumn(
        "order",
        from_json(
            col("raw_value"),
            order_schema
        )
    )
)

# ==================================================
# SCHEMA VALIDATION
# ==================================================

schema_failed = (
    order_df
    .filter(col("order").isNull())
    .withColumn("error_type", lit("SCHEMA_ERROR"))
    .withColumn("error_reason", lit("JSON_PARSE_FAILED"))
)

valid_schema = (
    order_df
    .filter(col("order").isNotNull())
)

# ==================================================
# FLATTEN
# ==================================================

valid_df = valid_schema.select(
    col("order.*"),
    col("topic").alias("source_topic"),
    col("partition").alias("source_partition"),
    col("offset").alias("source_offset"),
    to_timestamp("kafka_timestamp").alias("kafka_timestamp"),
    to_timestamp("ingestion_time").alias("bronze_ingestion_time")
)

# ==================================================
# NULL VALIDATION
# ==================================================

invalid_nulls = (
    valid_df.filter(
        col("event_id").isNull() |
        col("order_id").isNull() |
        col("customer_id").isNull() |
        col("product_id").isNull() |
        col("event_time").isNull()
    )
    .withColumn(
        "error_type",
        lit("NULL_VALIDATION")
    )
    .withColumn(
        "error_reason",
        lit("MANDATORY_FIELD_MISSING")
    )
)

valid_df = valid_df.filter(
    col("event_id").isNotNull() &
    col("order_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("product_id").isNotNull() &
    col("event_time").isNotNull()
)

# ==================================================
# BUSINESS VALIDATION
# ==================================================

invalid_business = (
    valid_df.filter(
        (col("quantity") <= 0) |
        (col("unit_price") <= 0) |
        (col("total_amount") <= 0) |
        (~col("order_id").startswith("ord_")) |
        (~col("customer_id").startswith("cust_")) |
        (~col("product_id").startswith("prod_")) |
        (abs(
            col("total_amount")
            - (col("quantity") * col("unit_price"))
        ) > 0.01)
    )
    .withColumn(
        "error_type",
        lit("BUSINESS_RULE")
    )
    .withColumn(
        "error_reason",
        lit("ORDER_VALIDATION_FAILED")
    )
)

valid_df = valid_df.filter(
    (col("quantity") > 0) &
    (col("unit_price") > 0) &
    (col("total_amount") > 0) &
    (col("order_id").startswith("ord_")) &
    (col("customer_id").startswith("cust_")) &
    (col("product_id").startswith("prod_")) &
    (
        abs(
            col("total_amount")
            - (col("quantity") * col("unit_price"))
        ) <= 0.01
    )
)

# ==================================================
# STANDARDIZATION
# ==================================================

valid_df = (
    valid_df
    .withColumn("event_type", upper(col("event_type")))
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("product_id", trim(col("product_id")))
)

# ==================================================
# DEDUPLICATION
# ==================================================

valid_df = (
    valid_df
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"])
)

# ==================================================
# DERIVED COLUMNS
# ==================================================

valid_df = valid_df.withColumn(
    "event_date",
    to_date("event_time")
)

# ==================================================
# DLQ STREAM
# ==================================================

dlq_df = (
    schema_failed.select(
        "raw_value",
        "error_type",
        "error_reason",
        col("topic").alias("source_topic"),
        col("partition").alias("source_partition"),
        col("offset").alias("source_offset"),
        to_timestamp("kafka_timestamp").alias("kafka_timestamp"),
        to_timestamp("ingestion_time").alias("bronze_ingestion_time")
    )
    .unionByName(
        invalid_nulls.select(
            to_json(struct("*")).alias("raw_value"),
            "error_type",
            "error_reason",
            "source_topic",
            "source_partition",
            "source_offset",
            "kafka_timestamp",
            "bronze_ingestion_time"
        )
    )
    .unionByName(
        invalid_business.select(
            to_json(struct("*")).alias("raw_value"),
            "error_type",
            "error_reason",
            "source_topic",
            "source_partition",
            "source_offset",
            "kafka_timestamp",
            "bronze_ingestion_time"
        )
    )
)

# ==================================================
# WRITE SILVER ICEBERG
# ==================================================

silver_query = (
    valid_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/home/jovyan/work/checkpoints/order_silver"
    )
    .toTable("local.silver.order_events")
)

# ==================================================
# WRITE DLQ ICEBERG
# ==================================================

dlq_query = (
    dlq_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/home/jovyan/work/checkpoints/order_silver_dlq"
    )
    .toTable("local.silver.order_events_dlq")
)

# ==================================================
# PUBLISH VALID EVENTS
# ==================================================

silver_topic_df = (
    valid_df.select(
        to_json(struct("*")).alias("value")
    )
)

silver_topic_query = (
    silver_topic_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("topic", "silver_order_events")
    .option(
        "checkpointLocation",
        "/home/jovyan/work/checkpoints/order_silver_topic"
    )
    .start()
)

# ==================================================
# PUBLISH DLQ EVENTS
# ==================================================

dlq_topic_df = (
    dlq_df.select(
        to_json(struct("*")).alias("value")
    )
)

dlq_topic_query = (
    dlq_topic_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("topic", "silver_order_events_dlq")
    .option(
        "checkpointLocation",
        "/home/jovyan/work/checkpoints/order_silver_dlq_topic"
    )
    .start()
)

spark.streams.awaitAnyTermination()