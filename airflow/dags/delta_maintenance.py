from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-eng',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'delta_maintenance',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    optimize = SparkSubmitOperator(
        task_id='optimize_silver_tables',
        application='/opt/spark-apps/delta_maintenance.py',  # A small script that runs OPTIMIZE
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.hadoop.fs.s3a.endpoint': 's3.eu-north-1.amazonaws.com',
        },
        application_args=['--action', 'optimize'],
    )

    vacuum = SparkSubmitOperator(
        task_id='vacuum_silver_tables',
        application='/opt/spark-apps/delta_maintenance.py',
        application_args=['--action', 'vacuum', '--retention-hours', '168'],
    )

    optimize >> vacuum  # VACUUM must run after OPTIMIZE