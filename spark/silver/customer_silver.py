# from pyspark.sql.functions import *

# from spark.common.spark_session import get_spark

# spark = get_spark()

# spark.sql(
#     """
#     CREATE DATABASE IF NOT EXISTS  local.silver
#     """
# )

# df = (
#     spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers",
#             "kafka:29092")
#     .option("subscribe","bronze_raw_customer_events")
#     .option("startingOffsets","earliest")
#     .option("failOnDataLoss","false")
#     .load()
# )