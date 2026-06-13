from pyspark.sql.functions import sum, col
from spark.common.spark_session import get_spark


def build_aggregates():

    spark = get_spark()

    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS local.gold
    """)

    orders = spark.table(
        "local.gold.fact_orders"
    )

    customers = spark.table(
        "local.gold.dim_customer"
    )

    products = spark.table(
        "local.gold.dim_product"
    )

    # --------------------------------------------------
    # Daily Sales
    # --------------------------------------------------

    daily_sales = (
        orders
        .groupBy("event_date")
        .agg(
            sum("total_amount").alias(
                "daily_revenue"
            )
        )
    )

    (
        daily_sales.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("local.gold.daily_sales")
    )

    print("daily_sales loaded")

    # --------------------------------------------------
    # Country Sales
    # --------------------------------------------------

    country_sales = (
        orders.alias("o")
        .join(
            customers.alias("c"),
            "customer_id"
        )
        .groupBy("country")
        .agg(
            sum("total_amount").alias(
                "revenue"
            )
        )
    )

    (
        country_sales.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("local.gold.country_sales")
    )

    print("country_sales loaded")

    # --------------------------------------------------
    # Product Sales
    # --------------------------------------------------

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
            sum("quantity").alias(
                "units_sold"
            ),
            sum("total_amount").alias(
                "revenue"
            )
        )
    )

    (
        product_sales.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("local.gold.product_sales")
    )

    print("product_sales loaded")

    # --------------------------------------------------
    # Customer Lifetime Value
    # --------------------------------------------------

    customer_ltv = (
        orders
        .groupBy("customer_id")
        .agg(
            sum("total_amount").alias(
                "lifetime_value"
            )
        )
    )

    (
        customer_ltv.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable(
            "local.gold.customer_lifetime_value"
        )
    )

    print("customer_lifetime_value loaded")

    # --------------------------------------------------
    # Category Sales
    # --------------------------------------------------

    category_sales = (
        orders.alias("o")
        .join(
            products.alias("p"),
            "product_id"
        )
        .groupBy("category")
        .agg(
            sum("total_amount").alias(
                "revenue"
            )
        )
    )

    (
        category_sales.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("local.gold.category_sales")
    )

    print("category_sales loaded")

    # --------------------------------------------------
    # Top Products
    # --------------------------------------------------

    top_products = (
        product_sales
        .orderBy(
            col("revenue").desc()
        )
        .limit(10)
    )

    (
        top_products.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("local.gold.top_products")
    )

    print("top_products loaded")