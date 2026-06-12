import subprocess
import time

jobs = [
    "spark.bronze.customer_bronze",
    "spark.bronze.order_bronze",
    "spark.bronze.product_bronze",

    "spark.silver.customer_silver",
    "spark.silver.order_silver",
    "spark.silver.product_silver"
]

processes = []

for job in jobs:

    print(f"Starting {job}")

    process = subprocess.Popen(
        ["python", "-m", job]
    )

    processes.append(process)

    time.sleep(5)

for process in processes:
    process.wait()