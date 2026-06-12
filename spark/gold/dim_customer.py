from spark.common.spark_session import get_spark

spark = get_spark()

customer_df = spark.table("local.silver.customer_events")

dim_customer = (
    customer_df
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "city",
        "event_date"
    )
    .dropDuplicates(["customer_id"])
)

(
    dim_customer.writeTo("local.gold.dim_customer")
    .using("iceberg")
    .createOrReplace()
)

print("dim_customer loaded")