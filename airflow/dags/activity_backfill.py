import os
import io
import pandas as pd
import json

import requests
from requests.exceptions import HTTPError

from datetime import timedelta

from collections import Counter

from common.preflight import preflight

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


BUCKET_NAME = os.getenv("BUCKET_NAME")
PORTS_URL = "https://hub.arcgis.com/api/v3/datasets/83b1bbc7b3354c5fb1f40673bb8f852e_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1"
CHOKEPOINTS_URL = "https://hub.arcgis.com/api/v3/datasets/3da2b9ca97684916b75c4013f95d18ab_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1"
PORTS_SERVICE_URL = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Ports_Data/FeatureServer/0"
CHOKEPOINTS_SERVICE_URL = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer/0"
AWS_CONN_ID = "aws_default"

if not BUCKET_NAME:
    raise RuntimeError("BUCKET_NAME is not set")

def get_source_count(service_url, where_clause="1=1"):

    r = requests.get(f"{service_url}/query", params={
        "where": where_clause,
        "returnCountOnly": "true",
        "f": "json",
    }, timeout=120)

    r.raise_for_status()

    body = r.json()

    if "count" not in body:          
        raise ValueError(f"Unexpected response: {body}")
    
    return body["count"]

@dag(
    dag_id="activity_backfill",
    schedule=None,
)
def activity_backfill_dag():

    @task(retries=0, execution_timeout=timedelta(hours=3))
    def backfill(url, ref_name, file_name, service_url, CHUNK_ROWS=100000):

        hook = S3Hook(aws_conn_id=AWS_CONN_ID) #initialize the S3Hook with the AWS connection ID

        try:

            year_counts = Counter()
            max_date = None
            total = 0

            #get the CSV data from the URL with a timeout of 300 seconds, streaming the response to avoid loading the entire file into memory at once
            with requests.get(url, stream=True, timeout=(15, 300)) as r:

                r.raise_for_status()
                r.raw.decode_content = True #ensure that the raw response is decoded properly

                reader = pd.read_csv(
                    r.raw,
                    dtype=str,                    #keep everything as text, no guessing
                    keep_default_na=False,        #"NA" stays as text,
                    chunksize=CHUNK_ROWS,
                    encoding="utf-8-sig"
                )

                for i, chunk in enumerate(reader):

                    if "year" not in chunk.columns:
                        raise ValueError(f"No 'year' column. Columns: {list(chunk.columns)}")
                    
                    for year, part in chunk.groupby("year", sort=False):

                        buffer = io.BytesIO()

                        part.drop(columns=["year"]).to_parquet(buffer, index=False)
                        hook.load_bytes(
                            bytes_data=buffer.getvalue(),
                            key=f"bronze/{ref_name}/{file_name}/year={int(year)}/part-{i:05d}.parquet",
                            bucket_name=BUCKET_NAME,
                            replace=True,
                        )

                    year_counts.update(chunk["year"].value_counts().to_dict())
                    dates = pd.to_datetime(chunk[["year", "month", "day"]].astype(int))
                    chunk_max = dates.max()
                    max_date = chunk_max if max_date is None else max(max_date, chunk_max)
                    total += len(chunk)

                    print(f"part {i}: {total:,} rows so far")

            expected_count = get_source_count(service_url) #get the expected row count from the source service
            upto_count = get_source_count(service_url, where_clause=(
                f"(year < {max_date.year}) OR "
                f"(year = {max_date.year} AND month < {max_date.month}) OR "
                f"(year = {max_date.year} AND month = {max_date.month} AND day <= {max_date.day})"
            ))

            print(f"snapshot rows={total:,}, max_date={max_date.date()}", flush=True)
            print(f"service rows up to that date={upto_count:,}; newer than snapshot={expected_count - upto_count:,}", flush=True)
            print(f"rows per year: {dict(sorted(year_counts.items()))}", flush=True)


            #check if the DataFrame is empty or if the row count does not match the expected count, and raise a ValueError if either condition is true
            if total == 0:
                raise ValueError("Downloaded CSV is empty")
            if abs(total - upto_count) > upto_count * 0.01:
                raise ValueError(f"Row count mismatch: got {total}, source says {upto_count}")

            # marker written last, so it only exists for a complete snapshot
            hook.load_string(
                json.dumps({"path": f"bronze/{ref_name}/{file_name}", "rows": total, "max_date": max_date.strftime("%Y-%m-%d")}),
                key=f"bronze/{ref_name}/_LATEST.json",
                bucket_name=BUCKET_NAME,
                replace=True,
            )

        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            raise
        except Exception as err:
            print(f"Other error occurred: {err}")
            raise

    gate = preflight(["BUCKET_NAME", "AIRFLOW_CONN_AWS_DEFAULT"], [PORTS_SERVICE_URL, CHOKEPOINTS_SERVICE_URL])
    ports = backfill.override(task_id="backfill_ports_activity")(url=PORTS_URL, ref_name="ports_activity", file_name="ports", service_url=PORTS_SERVICE_URL) #call the backfill task for ports reference data
    chokepoints = backfill.override(task_id="backfill_chokepoints_activity")(url=CHOKEPOINTS_URL, ref_name="chokepoints_activity", file_name="chokepoints", service_url=CHOKEPOINTS_SERVICE_URL) #call the backfill task for chokepoints reference data

    gate >> [ports, chokepoints] #set the dependencies so that the preflight checks must pass before the backfill tasks run

activity_backfill_dag()