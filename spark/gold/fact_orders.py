from spark.common.spark_session import get_spark


def build_fact_orders():

    spark = get_spark()

    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS local.gold
    """)

    orders_df = spark.table(
        "local.silver.order_events"
    )

    fact_orders = (
        orders_df
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "total_amount",
            "event_time",
            "event_date"
        )
    )

    (
        fact_orders.write
        .format("iceberg")
        .mode("overwrite")
        .partitionBy("event_date")
        .saveAsTable("local.gold.fact_orders")
    )

    print("fact_orders loaded")