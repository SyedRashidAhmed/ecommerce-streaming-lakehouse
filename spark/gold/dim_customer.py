from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
from spark.common.spark_session import get_spark


def build_dim_customer():

    spark = get_spark()

    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS local.gold
    """)

    customer_df = spark.table(
        "local.silver.customer_events"
    )

    window_spec = (
        Window
        .partitionBy("customer_id")
        .orderBy(desc("event_time"))
    )

    dim_customer = (
        customer_df
        .withColumn(
            "rn",
            row_number().over(window_spec)
        )
        .filter("rn = 1")
        .drop("rn")
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "country",
            "city",
            "event_time",
            "event_date"
        )
    )

    (
        dim_customer.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("local.gold.dim_customer")
    )

    print("dim_customer loaded")