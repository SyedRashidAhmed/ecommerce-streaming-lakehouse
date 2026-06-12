from spark.common.spark_session import get_spark
from pyspark.sql.functions import (
    sum,
    countDistinct,
    desc
)

spark = get_spark()

orders = spark.table("local.gold.fact_orders")
customers = spark.table("local.gold.dim_customer")
products = spark.table("local.gold.dim_product")

# Daily Sales
daily_sales = (
    orders
    .groupBy("event_date")
    .agg(
        sum("total_amount").alias("daily_revenue")
    )
)

(
    daily_sales.writeTo("local.gold.daily_sales")
    .using("iceberg")
    .createOrReplace()
)

# Country Sales
country_sales = (
    orders.alias("o")
    .join(
        customers.alias("c"),
        "customer_id"
    )
    .groupBy("country")
    .agg(
        sum("total_amount").alias("revenue")
    )
)

(
    country_sales.writeTo("local.gold.country_sales")
    .using("iceberg")
    .createOrReplace()
)

# Product Sales
product_sales = (
    orders.alias("o")
    .join(
        products.alias("p"),
        "product_id"
    )
    .groupBy(
        "product_id",
        "product_name",
        "category"
    )
    .agg(
        sum("quantity").alias("units_sold"),
        sum("total_amount").alias("revenue")
    )
)

(
    product_sales.writeTo("local.gold.product_sales")
    .using("iceberg")
    .createOrReplace()
)

# Customer Lifetime Value
customer_ltv = (
    orders
    .groupBy("customer_id")
    .agg(
        sum("total_amount").alias("lifetime_value")
    )
)

(
    customer_ltv.writeTo("local.gold.customer_lifetime_value")
    .using("iceberg")
    .createOrReplace()
)