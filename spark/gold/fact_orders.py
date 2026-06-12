from spark.common.spark_session import get_spark

spark = get_spark()

orders_df = spark.table("local.silver.order_events")

fact_orders = (
    orders_df
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "total_amount",
        "event_date"
    )
)

(
    fact_orders.writeTo("local.gold.fact_orders")
    .using("iceberg")
    .partitionedBy("event_date")
    .createOrReplace()
)

print("fact_orders loaded")