from spark.common.spark_session import get_spark

spark = get_spark()

product_df = spark.table("local.silver.product_events")

dim_product = (
    product_df
    .select(
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "unit_price",
        "event_date"
    )
    .dropDuplicates(["product_id"])
)

(
    dim_product.writeTo("local.gold.dim_product")
    .using("iceberg")
    .createOrReplace()
)

print("dim_product loaded")