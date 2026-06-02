from pyspark.sql import SparkSession


def get_spark():

    spark = (
        SparkSession.builder
        .appName("ecommerce-streaming")
        .config(
            "spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.local.type",
            "hadoop"
        )
        .config(
            "spark.sql.catalog.local.warehouse",
            "/home/jovyan/work/warehouse"
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
        )
        .getOrCreate()
    )

    return spark