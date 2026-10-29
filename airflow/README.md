# Airflow Batch Orchestration

This folder contains the local Apache Airflow 3.3.2 deployment, PortWatch
ingestion DAGs, Snowflake load and data-quality tasks, and the asset-triggered
dbt transformation DAG. The broader architecture and repository setup are in
the [root README](../README.md).

## What is here

- `docker-compose.yaml` runs Airflow with a CeleryExecutor, PostgreSQL metadata
  storage, and Redis. The API server/UI is available on port `8080`.
- `Dockerfile` installs the Amazon and Snowflake Airflow providers, plus a
  separate dbt Fusion executable under `/opt/dbt-venv`.
- `dags/` contains the ingestion and transformation DAGs and shared preflight,
  data-quality, ArcGIS, and output-verification helpers.
- `config/dbt/` contains the dbt profile used inside the Airflow containers.
- `tests/` contains DAG integrity and ArcGIS API tests.
- `pyproject.toml` and `uv.lock` define the local Python development and test
  environment. The Docker image installs its Airflow dependencies from
  `requirements.txt`.

## Configure and run locally

From this directory:

1. Copy `.env.example` to `.env`.
2. Set `AIRFLOW_UID` to your numeric user ID on Linux and set `FERNET_KEY` to a
   generated Fernet key.
3. Configure `AIRFLOW_CONN_AWS_DEFAULT` with an AWS connection that can access
   the S3 bucket and `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` with a Snowflake
   connection.
4. Set `BUCKET_NAME` and the `DBT_SNOWFLAKE_*` values for your deployment.
   Provide the private key at `DBT_SNOWFLAKE_PRIVATE_KEY_PATH`; Compose mounts
   the repository-level `keys/` folder read-only at `/opt/airflow/keys`.
5. Initialize Airflow and start the services:

   ```bash
   docker compose up airflow-init
   docker compose up -d
   ```

6. Open [http://localhost:8080](http://localhost:8080). The default UI
   credentials are `airflow` / `airflow`; change them in `.env` before
   exposing the deployment outside a disposable local environment.

The Compose file mounts `dags/`, `logs/`, `config/`, and `plugins/`, as well as
the repository dbt project and key directory. `.env` and private key material
must remain local and must not be committed. Stop the services with
`docker compose down`; use `-v` only when intentionally deleting the Airflow
metadata database volume.

This Compose configuration is for local development, not production. The
included services need at least 4 GB of memory and two CPUs available to
Docker.

## Shared preflight checks

The ingestion DAGs call a shared preflight task group. It validates required
environment variables, checks AWS identity and S3 write/read/delete access,
verifies that the configured ArcGIS services return data, and checks the
Snowflake connection. A failed check blocks downstream ingestion rather than
allowing a partial run to look successful.

## DAGs and data flow

### `activity_backfill`

- Manually triggered; use it to create the initial historical activity
  snapshots before relying on `activity_weekly`.
- Streams the ports and chokepoints activity CSV downloads and converts chunks
  to year-partitioned Parquet:
  - `bronze/ports_activity/ports/year=<year>/part-<chunk>.parquet`
  - `bronze/chokepoints_activity/chokepoints/year=<year>/part-<chunk>.parquet`
- Compares the downloaded row count with the ArcGIS service count through the
  snapshot's latest date. The allowed difference is at most 1%.
- Writes `bronze/<dataset>_activity/_LATEST.json` only after the snapshot
  validates. The marker contains the snapshot path, row count, and maximum
  date.

### `activity_weekly`

- Runs Wednesdays at `06:00 UTC`; catchup is disabled.
- Reads each S3 watermark, or uses the snapshot marker as the initial
  watermark, and re-fetches a 14-day overlap to account for source revisions.
- Pages through the ArcGIS services, validates fetched counts, writes
  `part-weekly-<UTC-date>.parquet` files under each dataset's year partition,
  and advances the watermark only after the write succeeds.
- Verifies the two watermark outputs, then runs the `ports_silver_weekly` Glue
  job to normalize and quality-check PortWatch ports activity.
- Calls `LOAD_PORTS_ACTIVITY(14)` and `LOAD_AIS_DATA(14)` in Snowflake, runs
  the PortWatch and AIS data-quality checks, and finally runs Delta
  maintenance for the AIS position and vessel tables.
- Emits Airflow assets for the ports and AIS data only after their corresponding
  quality checks pass. Those assets schedule `dbt_transform`.

The weekly DAG also fetches chokepoints activity, but the current downstream
Snowflake/dbt models in this repository focus on ports activity and AIS.

### `ingest_reference_data`

- Runs monthly from the configured start date of October 2026; catchup is
  disabled. It can also be triggered manually.
- Downloads complete ports and chokepoints reference CSVs and compares row
  counts with the corresponding ArcGIS services.
- Writes Parquet files to
  `bronze/ports_reference/ports.parquet` and
  `bronze/chokepoints_reference/chokepoints.parquet`, verifies that both were
  created, and runs the `ports_ref_silver` Glue job.

This DAG updates S3; Snowflake's initial load or a separately managed refresh
is needed to copy updated reference data into Snowflake.

### `dbt_transform`

- Is scheduled by the `RAW_PORTS_ACTIVITY` and `RAW_AIS` Airflow assets emitted
  by successful weekly load and quality-check tasks.
- Runs `dbt deps`, checks source freshness, then builds/tests staging and
  intermediate models followed by marts.
- Uses the `prod` target from `config/dbt/profiles.yml`. Credentials are read
  from environment variables and the private key file mounted into the
  container.

## Tests

From this directory, install the locked environment, lint, and run tests:

```bash
uv sync
uv run ruff check .
uv run --group test pytest tests/ -v
```

The tests cover DAG imports and conventions and the shared ArcGIS query helper;
they do not require a live cloud deployment.

## Monitoring and cleanup

Use the Airflow UI to inspect DAG runs and task logs. The Compose file also
defines an `airflow-cli` debug profile and an optional Flower service for
Celery monitoring. To stop local Airflow, run:

```bash
docker compose down
```

Do not use the development Compose credentials or expose its services on an
untrusted network.