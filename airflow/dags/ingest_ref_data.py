import os
import io
import pandas as pd

import requests
from requests.exceptions import HTTPError

from datetime import datetime, timedelta

from common.preflight import preflight
from common.verify import verify_outputs
from common.portwatch_api import query

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

BUCKET_NAME = os.getenv("BUCKET_NAME")
PORTS_URL = "https://hub.arcgis.com/api/v3/datasets/acc668d199d1472abaaf2467133d4ca4_0/downloads/data?format=csv&spatialRefId=3857&where=1%3D1"
CHOKEPOINTS_URL = "https://hub.arcgis.com/api/v3/datasets/fa9a5800b0ee4855af8b2944ab1e07af_0/downloads/data?format=csv&spatialRefId=3857&where=1%3D1"
PORTS_SERVICE_URL = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/PortWatch_ports_database/FeatureServer/0"
CHOKEPOINTS_SERVICE_URL = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/PortWatch_chokepoints_database/FeatureServer/0"
AWS_CONN_ID = "aws_default"


@dag(
    dag_id="ingest_reference_data",
    schedule="@monthly",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["reference_data", "ports", "chokepoints"],
)
def ingest_ports_ref_dag():

    @task
    def ingest(url, ref_name, file_name, service_url):

        hook = S3Hook(aws_conn_id=AWS_CONN_ID) #initialize the S3Hook with the AWS connection ID

        try:

            expected_count = query({"where": "1=1", "returnCountOnly": "true"}, service_url)["count"] #get the expected row count from the source service
            r = requests.get(url, timeout=120) #get the CSV data from the URL with a timeout of 120 seconds

            r.raise_for_status() #raise an HTTPError if the HTTP request returned an unsuccessful status code

            #read the CSV data into a DataFrame, treating all columns as strings and preventing empty strings from being converted to NaN
            df = pd.read_csv(io.BytesIO(r.content), dtype=str, keep_default_na=False, encoding="utf-8-sig") 

            #check if the DataFrame is empty or if the row count does not match the expected count, and raise a ValueError if either condition is true
            if len(df) == 0:
                raise ValueError("Downloaded CSV is empty")
            if len(df) != expected_count:
                raise ValueError(f"Row count mismatch: got {len(df)}, source says {expected_count}")
            
            buffer = io.BytesIO() #buffer to hold the Parquet data in memory

            df.to_parquet(buffer, index=False) #convert the DataFrame to Parquet format and write it to the buffer
            #upload the Parquet data from the buffer to S3, replacing any existing file with the same key
            hook.load_bytes(
                bytes_data=buffer.getvalue(),
                key=f"bronze/{ref_name}/{file_name}.parquet",
                bucket_name=BUCKET_NAME,
                replace=True,
            )

        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            raise
        except Exception as err:
            print(f"Other error occurred: {err}")
            raise

    silver_ports_ref = GlueJobOperator(
        task_id="silver_ports_ref",
        job_name="ports_ref_silver", 
        wait_for_completion=True,
        region_name="eu-north-1",
        verbose=False,
        retries=1,
    )
    gate = preflight(["BUCKET_NAME", "AIRFLOW_CONN_AWS_DEFAULT"], [PORTS_SERVICE_URL, CHOKEPOINTS_SERVICE_URL])
    ports = ingest.override(task_id="ingest_ports")(url=PORTS_URL, ref_name="ports_reference", file_name="ports", service_url=PORTS_SERVICE_URL) #call the ingest task for ports reference data
    chokepoints = ingest.override(task_id="ingest_chokepoints")(url=CHOKEPOINTS_URL, ref_name="chokepoints_reference", file_name="chokepoints", service_url=CHOKEPOINTS_SERVICE_URL) #call the ingest task for chokepoints reference data

    gate >> [ports, chokepoints] >> verify_outputs([
    "bronze/ports_reference/ports.parquet",
    "bronze/chokepoints_reference/chokepoints.parquet",
    ]) >> silver_ports_ref

ingest_ports_ref_dag()