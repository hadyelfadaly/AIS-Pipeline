import os

from airflow.sdk import task, get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@task(trigger_rule="all_done", retries=0)
def verify_outputs(keys):

    bucket = os.getenv("BUCKET_NAME")
    hook = S3Hook(aws_conn_id="aws_default")
    run_started = get_current_context()["dag_run"].start_date

    problems = []

    for key in keys:

        meta = hook.head_object(key=key, bucket_name=bucket)  #None if missing

        if meta is None:
            problems.append(f"missing: {key}")
        elif meta["ContentLength"] == 0:
            problems.append(f"empty: {key}")
        elif meta["LastModified"] < run_started:
            problems.append(f"not written during this run: {key}")

    if problems:
        raise RuntimeError("Output verification failed: " + "; ".join(problems))
    
    print(f"Verified {len(keys)} outputs in s3://{bucket}")