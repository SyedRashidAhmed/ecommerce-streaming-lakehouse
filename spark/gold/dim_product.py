from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
from spark.common.spark_session import get_spark


def build_dim_product():

    spark = get_spark()

    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS local.gold
    """)

    product_df = spark.table(
        "local.silver.product_events"
    )

    window_spec = (
        Window
        .partitionBy("product_id")
        .orderBy(desc("event_time"))
    )

    dim_product = (
        product_df
        .withColumn(
            "rn",
            row_number().over(window_spec)
        )
        .filter("rn = 1")
        .drop("rn")
        .select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "unit_price",
            "event_time",
            "event_date"
        )
    )

    (
        dim_product.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("local.gold.dim_product")
    )

    print("dim_product loaded")