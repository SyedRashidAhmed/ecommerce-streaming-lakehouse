from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="gold_layer_refresh",
    start_date=datetime(2026, 6, 13),
    schedule="@hourly",
    catchup=False,
    tags=["iceberg", "gold"]
) as dag:

    refresh_gold = BashOperator(
        task_id="refresh_gold",
        bash_command="""
        docker exec jupyter bash -lc "
        export PYTHONPATH=/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip:/usr/local/spark/python:/home/jovyan/work &&
        cd /home/jovyan/work &&
        python -m spark.gold.run_all
        "
        """
    )