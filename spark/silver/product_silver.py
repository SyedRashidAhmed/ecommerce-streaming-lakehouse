from pyspark.sql.functions import *
from pyspark.sql.types import *

from spark.common.spark_session import get_spark

spark = get_spark()

# ==========================================================
# DATABASE
# ==========================================================

spark.sql("""
CREATE DATABASE IF NOT EXISTS local.silver
""")

# ==========================================================
# CREATE TABLES
# ==========================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS local.silver.product_events (

    event_id STRING,
    event_type STRING,
    event_time TIMESTAMP,

    product_id STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,

    unit_price DOUBLE,

    source_topic STRING,
    source_partition INT,
    source_offset BIGINT,

    kafka_timestamp TIMESTAMP,
    bronze_ingestion_time TIMESTAMP,

    event_date DATE

)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.silver.product_events_dlq (

    event_id STRING,
    event_type STRING,
    event_time TIMESTAMP,

    product_id STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,

    unit_price DOUBLE,

    source_topic STRING,
    source_partition INT,
    source_offset BIGINT,

    kafka_timestamp TIMESTAMP,
    bronze_ingestion_time TIMESTAMP,

    event_date DATE,

    error_reason STRING

)
USING iceberg
""")

# ==========================================================
# PRODUCT SCHEMA
# ==========================================================

product_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("subcategory", StringType()),
    StructField("brand", StringType()),
    StructField("unit_price", DoubleType())
])

# ==========================================================
# READ BRONZE TABLE
# ==========================================================

bronze_df = (
    spark.readStream
    .format("iceberg")
    .load("local.bronze.product_events")
)

# ==========================================================
# PARSE RAW JSON
# ==========================================================

parsed_df = (
    bronze_df
    .select(
        from_json(
            col("raw_value"),
            product_schema
        ).alias("product"),

        col("topic").alias("source_topic"),
        col("partition").alias("source_partition"),
        col("offset").alias("source_offset"),

        to_timestamp(
            col("kafka_timestamp")
        ).alias("kafka_timestamp"),

        to_timestamp(
            col("ingestion_time")
        ).alias("bronze_ingestion_time")
    )
)

product_df = (
    parsed_df
    .select(
        "product.*",

        "source_topic",
        "source_partition",
        "source_offset",

        "kafka_timestamp",
        "bronze_ingestion_time"
    )
)

# ==========================================================
# STANDARDIZATION
# ==========================================================

product_df = (

    product_df

    .withColumn(
        "event_type",
        upper(trim(col("event_type")))
    )

    .withColumn(
        "product_name",
        trim(col("product_name"))
    )

    .withColumn(
        "category",
        upper(trim(col("category")))
    )

    .withColumn(
        "subcategory",
        upper(trim(col("subcategory")))
    )

    .withColumn(
        "brand",
        upper(trim(col("brand")))
    )

    .withColumn(
        "event_time",
        to_timestamp("event_time")
    )

    .withColumn(
        "event_date",
        to_date("event_time")
    )
)

# ==========================================================
# VALID RECORDS
# ==========================================================

valid_df = (

    product_df

    .filter(col("product_id").isNotNull())

    .filter(col("product_name").isNotNull())

    .filter(col("category").isNotNull())

    .filter(col("subcategory").isNotNull())

    .filter(col("brand").isNotNull())

    .filter(col("unit_price").isNotNull())

    .filter(col("unit_price") > 0)

    .filter(col("event_type") == "PRODUCT_CREATED")

    .withWatermark(
        "event_time",
        "10 minutes"
    )

    .dropDuplicates(
        ["product_id"]
    )
)

# ==========================================================
# INVALID RECORDS
# ==========================================================

invalid_df = (

    product_df

    .withColumn(

        "error_reason",

        when(
            col("product_id").isNull(),
            "NULL_PRODUCT_ID"
        )

        .when(
            col("product_name").isNull(),
            "NULL_PRODUCT_NAME"
        )

        .when(
            col("category").isNull(),
            "NULL_CATEGORY"
        )

        .when(
            col("subcategory").isNull(),
            "NULL_SUBCATEGORY"
        )

        .when(
            col("brand").isNull(),
            "NULL_BRAND"
        )

        .when(
            col("unit_price").isNull(),
            "NULL_UNIT_PRICE"
        )

        .when(
            col("unit_price") <= 0,
            "INVALID_UNIT_PRICE"
        )

        .when(
            col("event_type") != "PRODUCT_CREATED",
            "INVALID_EVENT_TYPE"
        )
    )

    .filter(
        col("error_reason").isNotNull()
    )
)

# ==========================================================
# SILVER TABLE
# ==========================================================

silver_query = (

    valid_df

    .writeStream

    .format("iceberg")

    .outputMode("append")

    .option(
        "checkpointLocation",
        "/home/jovyan/work/checkpoints/product_silver"
    )

    .toTable(
        "local.silver.product_events"
    )
)

# ==========================================================
# DLQ TABLE
# ==========================================================

dlq_query = (

    invalid_df

    .writeStream

    .format("iceberg")

    .outputMode("append")

    .option(
        "checkpointLocation",
        "/home/jovyan/work/checkpoints/product_silver_dlq"
    )

    .toTable(
        "local.silver.product_events_dlq"
    )
)

spark.streams.awaitAnyTermination()