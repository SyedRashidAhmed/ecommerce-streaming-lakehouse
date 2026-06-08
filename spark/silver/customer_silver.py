from pyspark.sql.functions import *
from pyspark.sql.types import *

from spark.common.spark_session import get_spark

spark = get_spark()

# ------------------------------------------------
# Create Silver Database
# ------------------------------------------------

spark.sql("""
CREATE DATABASE IF NOT EXISTS local.silver
""")

# ------------------------------------------------
# Silver Table
# ------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS local.silver.customer_events (

    event_id STRING,
    event_type STRING,
    event_time TIMESTAMP,

    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,

    country STRING,
    city STRING,

    source_topic STRING,
    source_partition INT,
    source_offset BIGINT,

    kafka_timestamp TIMESTAMP,
    bronze_ingestion_time TIMESTAMP,

    event_date DATE

)
USING iceberg
PARTITIONED BY (event_date)
""")

# ------------------------------------------------
# DLQ Table
# ------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS local.silver.customer_events_dlq (

    raw_value STRING,

    error_type STRING,
    error_reason STRING,

    source_topic STRING,
    source_partition INT,
    source_offset BIGINT,

    kafka_timestamp TIMESTAMP,
    bronze_ingestion_time TIMESTAMP

)
USING iceberg
""")

# ------------------------------------------------
# Schema
# ------------------------------------------------

customer_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("customer_id", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType())
])

# ------------------------------------------------
# Read Bronze Kafka Topic
# ------------------------------------------------

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "bronze_raw_customer_events")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss","false")
    .load()
)

# ------------------------------------------------
# Parse Bronze Payload
# ------------------------------------------------

bronze_df = (
    df
    .selectExpr("CAST(value AS STRING) as value")
)

parsed_df = (
    bronze_df
    .withColumn(
        "bronze",
        from_json(
            col("value"),
            StructType([
                StructField("raw_value", StringType()),
                StructField("topic", StringType()),
                StructField("partition", IntegerType()),
                StructField("offset", LongType()),
                StructField("kafka_timestamp", StringType()),
                StructField("ingestion_time", StringType())
            ])
        )
    )
)

flattened_df = parsed_df.select(
    "bronze.*"
)

# ------------------------------------------------
# Parse Actual Customer JSON
# ------------------------------------------------

customer_df = (
    flattened_df
    .withColumn(
        "customer",
        from_json(
            col("raw_value"),
            customer_schema
        )
    )
)

# ------------------------------------------------
# Schema Enforcement
# ------------------------------------------------

schema_failed = (
    customer_df
    .filter(col("customer").isNull())
    .withColumn("error_type", lit("SCHEMA_ERROR"))
    .withColumn("error_reason", lit("JSON_PARSE_FAILED"))
)

valid_schema = (
    customer_df
    .filter(col("customer").isNotNull())
)

# ------------------------------------------------
# Flatten
# ------------------------------------------------

valid_df = valid_schema.select(
    col("customer.*"),
    col("topic").alias("source_topic"),
    col("partition").alias("source_partition"),
    col("offset").alias("source_offset"),
    to_timestamp("kafka_timestamp").alias("kafka_timestamp"),
    to_timestamp("ingestion_time").alias("bronze_ingestion_time")
)

# ------------------------------------------------
# Null Filtering
# ------------------------------------------------

invalid_nulls = valid_df.filter(
    col("event_id").isNull() |
    col("customer_id").isNull() |
    col("email").isNull()
).withColumn(
    "error_type",
    lit("NULL_VALIDATION")
).withColumn(
    "error_reason",
    lit("MANDATORY_FIELD_MISSING")
)

valid_df = valid_df.filter(
    col("event_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("email").isNotNull()
)

# ------------------------------------------------
# Business Rule Validation
# ------------------------------------------------

invalid_business = valid_df.filter(
    ~col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
).withColumn(
    "error_type",
    lit("BUSINESS_RULE")
).withColumn(
    "error_reason",
    lit("INVALID_EMAIL")
)

valid_df = valid_df.filter(
    col("email").rlike(
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )
)

# ------------------------------------------------
# Standardization
# ------------------------------------------------

valid_df = (
    valid_df
    .withColumn("email", lower(col("email")))
    .withColumn("country", upper(col("country")))
    .withColumn("first_name", initcap(col("first_name")))
    .withColumn("last_name", initcap(col("last_name")))
)

# ------------------------------------------------
# Deduplication
# ------------------------------------------------

valid_df = (
    valid_df
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"])
)

# ------------------------------------------------
# Derived Columns
# ------------------------------------------------

valid_df = valid_df.withColumn(
    "event_date",
    to_date("event_time")
)

# ------------------------------------------------
# DLQ Stream
# ------------------------------------------------

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

# ------------------------------------------------
# Silver Write
# ------------------------------------------------

silver_query = (
    valid_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/home/jovyan/work/checkpoints/customer_silver"
    )
    .toTable("local.silver.customer_events")
)

# ------------------------------------------------
# DLQ Write
# ------------------------------------------------

dlq_query = (
    dlq_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/home/jovyan/work/checkpoints/customer_silver_dlq"
    )
    .toTable("local.silver.customer_events_dlq")
)

from pyspark.sql.functions import to_json,struct
CUSTOMER_SILVER = "silver_customer_events"

output_df = (
    valid_df.select(
        to_json(
            struct("*")
        ).alias("value")
    )
)

output_df1 = (
    output_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers","kafka:29092")
    .option("topic",CUSTOMER_SILVER)
    .option("checkpointLocation",
            "/home/jovyan/work/checkpoints/customer_silver_topic")
    .start()

)

CUSTOMER_SILVER_DLQ = "silver_customer_events_dlq"
output2_df = (
    dlq_df.select(
        to_json(
            struct("*")
        ).alias("value")
    )
)

output_df3 = (
    output2_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers","kafka:29092")
    .option("topic",CUSTOMER_SILVER_DLQ)
    .option("checkpointLocation",
            "/home/jovyan/work/checkpoints/customer_silver_topic_dlq")
    .start()

)

spark.streams.awaitAnyTermination()