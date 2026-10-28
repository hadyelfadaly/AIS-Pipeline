import io
import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

from common.preflight import preflight
from common.verify import verify_outputs
from common.portwatch_api import query

BUCKET_NAME = os.getenv("BUCKET_NAME")
AWS_CONN_ID = "aws_default"
PORTS_SERVICE_URL = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Ports_Data/FeatureServer/0"
CHOKEPOINTS_SERVICE_URL = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer/0"
PORTS_BASE = "bronze/ports_activity"
CHOKEPOINTS_BASE = "bronze/chokepoints_activity"
PORTS_SNAPSHOT_PREFIX = f"{PORTS_BASE}/ports"             #written by activity_backfill
CHOKEPOINTS_SNAPSHOT_PREFIX = f"{CHOKEPOINTS_BASE}/chokepoints"   #written by activity_backfill
PORTS_SNAPSHOT_MARKER_KEY = f"{PORTS_BASE}/_LATEST.json"  #written by activity_backfill
CHOKEPOINTS_SNAPSHOT_MARKER_KEY = f"{CHOKEPOINTS_BASE}/_LATEST.json"  #written by activity_backfill
PORTS_STATE_KEY = f"{PORTS_BASE}/_state/watermark.json"   #rewritten by every weekly run
CHOKEPOINTS_STATE_KEY = f"{CHOKEPOINTS_BASE}/_state/watermark.json"   #rewritten by every weekly run
PORTS_WEEKLY_PREFIX = f"{PORTS_BASE}/weekly"
CHOKEPOINTS_WEEKLY_PREFIX = f"{CHOKEPOINTS_BASE}/weekly"
OVERLAP_DAYS = 14            #estimates can be revised, so re-fetch recent days
PAGE_SIZE = 1000             #the service's maxRecordCount
ORDER_BY = "ObjectId"   #must be unique per row, or paging can skip rows
DROP_COLUMNS = {"fid", "esri_oid"}   #ArcGIS bookkeeping columns


def where_from(d):
    
    return (
        f"(year > {d.year}) OR (year = {d.year} AND month > {d.month}) "
        f"OR (year = {d.year} AND month = {d.month} AND day >= {d.day})"
    )


def snapshot_max_date(hook, snapshot_prefix, snapshot_marker_key):
    
    if hook.check_for_key(snapshot_marker_key, bucket_name=BUCKET_NAME):

        marker = json.loads(hook.read_key(snapshot_marker_key, bucket_name=BUCKET_NAME))

        if marker.get("max_date"):
            return date.fromisoformat(marker["max_date"])

    prefixes = hook.list_prefixes(bucket_name=BUCKET_NAME, prefix=f"{snapshot_prefix}/", delimiter="/") or []
    years = sorted(int(p.rstrip("/").split("year=")[1]) for p in prefixes if "year=" in p)

    if not years:
        raise RuntimeError(f"No year=... folders under {snapshot_prefix}/")

    year = years[-1]
    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=f"{snapshot_prefix}/year={year}/") or []
    frames = []

    for key in keys:

        if key.endswith(".parquet"):

            body = hook.get_key(key, bucket_name=BUCKET_NAME).get()["Body"].read()

            frames.append(pd.read_parquet(io.BytesIO(body), columns=["month", "day"]))

    if not frames:
        raise RuntimeError(f"No parquet files found for year={year}")

    md = pd.concat(frames).astype(int).sort_values(["month", "day"]).iloc[-1]

    return date(year, int(md["month"]), int(md["day"]))


def read_watermark(hook, state_key, snapshot_prefix, snapshot_marker_key):

    if hook.check_for_key(state_key, bucket_name=BUCKET_NAME):

        state = json.loads(hook.read_key(state_key, bucket_name=BUCKET_NAME))

        return date.fromisoformat(state["watermark"])
    
    return snapshot_max_date(hook, snapshot_prefix, snapshot_marker_key) #if no state file, use the snapshot's max date as the watermark


@dag(
    dag_id="activity_weekly",
    schedule="0 6 * * 3",            #Wednesdays 06:00 UTC, a day after the expected refresh
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["activity", "ports", "chokepoints"]
)
def activity_weekly_dag():

    @task
    def pull_activity(service_url, state_key, snapshot_prefix, snapshot_marker_key):

        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        watermark = read_watermark(hook, state_key, snapshot_prefix, snapshot_marker_key)
        start = watermark - timedelta(days=OVERLAP_DAYS)
        where = where_from(start)

        print(f"watermark={watermark}, pulling from {start}", flush=True)

        expected_count = query({"where": where, "returnCountOnly": "true"}, service_url)["count"]

        rows, last_id = [], -1

        while True:

            body = query({
                "where": f"({where}) AND ObjectId > {last_id}",
                "outFields": "*",
                "returnGeometry": "false",
                "orderByFields": "ObjectId",
                "resultRecordCount": PAGE_SIZE,
            }, service_url)

            features = body.get("features", [])
            if not features:
                break

            rows.extend(f["attributes"] for f in features)
            last_id = features[-1]["attributes"]["ObjectId"]
            print(f"{len(rows):,} / {expected_count:,}", flush=True)

            if len(rows) >= expected_count:   # everything fetched: don't request an empty page
                break

            time.sleep(0.2)

        print(f"fetched {len(rows):,} rows, service says {expected_count:,}", flush=True)

        #With a 14-day overlap there are always rows, so zero means something is wrong.
        if not rows:
            raise ValueError("No rows returned, even with the overlap window")
        if len(rows) != expected_count:
            raise ValueError(f"Row count mismatch: got {len(rows):,}, service says {expected_count:,}")

        df = pd.DataFrame(rows)
        df = df.drop(columns=[c for c in df.columns if c.lower() in DROP_COLUMNS])
        max_date = pd.to_datetime(df[["year", "month", "day"]].astype(int)).max().date()
        df = df.astype("string")
        ingest_date = datetime.now(timezone.utc).date().isoformat()
        written = []

        for year, part in df.groupby("year", sort=False):

            key = f"{snapshot_prefix}/year={int(year)}/part-weekly-{ingest_date}.parquet"
            buffer = io.BytesIO()

            part.drop(columns=["year"]).to_parquet(buffer, index=False)
            hook.load_bytes(
                bytes_data=buffer.getvalue(),
                key=key,
                bucket_name=BUCKET_NAME,
                replace=True,        #a rerun on the same day overwrites the same file
            )
            written.append(key)


        new_watermark = max(watermark, max_date)  #state is written last, so the watermark only moves after a good write

        hook.load_string(
            json.dumps({
                "watermark": new_watermark.isoformat(),
                "rows_last_run": len(rows),
                "last_files": written,
                "last_run_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "overlap_days": OVERLAP_DAYS,
            }),
            key=state_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

        print(f"wrote {len(written)} file(s): {written}; watermark is now {new_watermark}", flush=True)

    silver_ports_activity = GlueJobOperator(
        task_id="silver_ports_activity",
        job_name="ports_silver_weekly",
        script_args={"--lookback_days": "14"},
        region_name="eu-north-1",
        wait_for_completion=True,
        verbose=False,
        retries=1,
    )
    gate = preflight(["BUCKET_NAME", "AIRFLOW_CONN_AWS_DEFAULT"], [PORTS_SERVICE_URL, CHOKEPOINTS_SERVICE_URL])
    pull = [pull_activity.override(task_id="pull_ports_activity")( service_url=PORTS_SERVICE_URL, state_key=PORTS_STATE_KEY, snapshot_prefix=PORTS_SNAPSHOT_PREFIX, snapshot_marker_key=PORTS_SNAPSHOT_MARKER_KEY),
            pull_activity.override(task_id="pull_chokepoints_activity")(service_url=CHOKEPOINTS_SERVICE_URL, state_key=CHOKEPOINTS_STATE_KEY, snapshot_prefix=CHOKEPOINTS_SNAPSHOT_PREFIX, snapshot_marker_key=CHOKEPOINTS_SNAPSHOT_MARKER_KEY)]

    gate >> pull >> verify_outputs([PORTS_STATE_KEY, CHOKEPOINTS_STATE_KEY]) >> silver_ports_activity


activity_weekly_dag()