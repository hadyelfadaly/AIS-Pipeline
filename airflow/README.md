# Airflow

This folder contains the local Apache Airflow deployment and the DAGs that ingest
PortWatch ports and chokepoints data into the project's S3 bronze layer.

## What is here

- `docker-compose.yaml` runs Airflow with a CeleryExecutor, backed by PostgreSQL
  and Redis. The API server is available on port `8080`.
- `Dockerfile` builds an Airflow `3.3.2` image and installs the Python packages
  from `requirements.txt`.
- `dags/` contains the DAGs and shared preflight/output-verification tasks.
- `config/` and `plugins/` are mounted into the Airflow containers. They are
  currently placeholders for local configuration and plugins.
- `pyproject.toml` and `uv.lock` describe the Python project and its locked
  dependencies. The Docker image currently installs from `requirements.txt`.

## Run locally

Run these commands from this folder:

1. If you do not already have a local `.env`, copy `.env.example` to `.env`.
2. Edit `.env` and set:
   - `AIRFLOW_UID` to your numeric user ID on Linux (for example, the output of
     `id -u`).
   - `FERNET_KEY` to a generated Fernet key.
   - `AIRFLOW_CONN_AWS_DEFAULT` to an Airflow AWS connection JSON value with
     credentials and a region that can access the target bucket.
   - `BUCKET_NAME` to the destination S3 bucket.
3. Initialize the Airflow metadata database and local directories, then start
   the services:

   ```bash
   docker compose up airflow-init
   docker compose up -d
   ```

4. Open [http://localhost:8080](http://localhost:8080) and sign in. The Compose
   initialization defaults the web UI user name and password to `airflow` /
   `airflow`; configure your own values before exposing this local deployment
   beyond your machine.

The Compose file loads `.env` into the services and mounts `dags/`, `logs/`,
`config/`, and `plugins/`. The `.env` file is local configuration and should not
be committed. To stop the services, run `docker compose down`; add `-v` only if
you intentionally want to remove the PostgreSQL metadata volume.

This Compose configuration is intended for local development, not production.
The deployment uses a PostgreSQL metadata database and Redis-backed Celery
workers; Docker should have at least 4 GB of memory and two CPUs available.

## DAGs and data flow

All DAGs first run a shared `preflight` task group. It checks required
environment variables, verifies AWS identity, writes and removes an S3
health-check object, and checks that each ArcGIS source responds with rows.

### `activity_backfill`

- Manually triggered (`schedule=None`); run this once to seed historical
  activity data before relying on weekly ingestion.
- Downloads the ports and chokepoints activity CSVs as streams, processes them
  in chunks, and writes Parquet files partitioned by year:
  - `bronze/ports_activity/ports/year=<year>/part-<chunk>.parquet`
  - `bronze/chokepoints_activity/chokepoints/year=<year>/part-<chunk>.parquet`
- Compares the downloaded snapshot row count with the ArcGIS service's count
  through the snapshot's latest date (allowing a 1% difference).
- Writes a `bronze/<dataset>_activity/_LATEST.json` marker only after the
  snapshot passes validation. The marker records the snapshot path, row count,
  and maximum date.

### `activity_weekly`

- Runs every Wednesday at `06:00 UTC`; catchup is disabled.
- For each activity dataset, starts from its stored S3 watermark, or falls back
  to the backfill snapshot's latest date when no watermark exists.
- Re-fetches a 14-day overlap to account for revisions, pages through the
  ArcGIS service, and checks the fetched count against the service count.
- Writes yearly Parquet files under
  `bronze/<dataset>_activity/<dataset>/year=<year>/part-weekly-<UTC-date>.parquet`.
  The datasets are `ports` and `chokepoints`.
- Updates `bronze/<dataset>_activity/_state/watermark.json` after writing the
  data. The state includes the watermark, rows fetched, file keys, run time, and
  overlap size. A final task verifies that both state objects were updated in
  the current run.

### `ingest_reference_data`

- Runs monthly, starting October 2026; catchup is disabled.
- Downloads the complete ports and chokepoints reference datasets, checks each
  row count against its ArcGIS service, and stores Parquet files at:
  - `bronze/ports_reference/ports.parquet`
  - `bronze/chokepoints_reference/chokepoints.parquet`
- A final task checks that both output objects exist, are non-empty, and were
  written during the current run.

Each task uses the `aws_default` Airflow connection and the `BUCKET_NAME`
environment variable to access S3. Failed source requests, empty downloads, and
row-count mismatches fail the relevant task rather than being treated as
successful ingestions.

## Running and monitoring DAGs

After the services are up, trigger `activity_backfill` manually from the Airflow
UI to create the initial activity snapshot. The weekly DAG can then use that
snapshot as its initial watermark. The reference-data DAG runs on its monthly
schedule, or can be triggered manually from the UI.

Use the Airflow UI to monitor DAG and task runs and inspect task logs. The
Compose configuration also includes an `airflow-cli` debug profile and an
optional Flower service for Celery monitoring.