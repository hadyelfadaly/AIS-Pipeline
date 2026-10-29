# AIS dbt Warehouse Models

This dbt project transforms the curated PortWatch and AIS tables in Snowflake
into tested analytical models. The repository-level pipeline overview and
environment setup are documented in the [root README](../../README.md).

## Project layout

- `models/staging/`: typed and renamed source views for PortWatch activity,
  port reference data, AIS positions, and AIS vessel static messages.
- `models/intermediate/`: reusable analytical logic, including cargo/tanker
  position enrichment and port-congestion calculations.
- `models/marts/core/`: warehouse dimensions and business facts.
- `macros/`: shared SQL helpers, including schema naming and geodesic distance
  calculations.
- `packages.yml`: dbt package dependencies (`dbt_utils`).

The configured materializations use views for staging and tables for
intermediate and mart models. The default profile target is Snowflake `prod`,
with models in the `AIS.STAGING` and `AIS.MARTS` schemas.

## Sources and models

The project reads Snowflake `AIS.RAW` sources:

- `PORTS_ACTIVITY` and `PORTS_REF`
- `AIS_POSITIONS` and `AIS_VESSELS`

The current marts are:

- `dim_ports`: port locations and trade profile.
- `dim_vessels`: the most recent static message per vessel.
- `fct_port_activity_daily`: daily port calls and import/export volumes.
- `fct_vessel_telemetry`: cargo/tanker position history with weather and nearby
  port attributes.
- `fct_vessel_eta_risk`: destination match, projected arrival, ETA status, and
  candidate weather/congestion context.
- `mart_weather_speed_impact`: vessel speed comparison by sea state.

Notable business rules include keeping missing weather unknown, selecting the
latest vessel static message, matching destinations by UN/LOCODE or unique
port name, and treating straight-line distance as an approximation for ETA
projections. Model and column tests are defined alongside the models in
`schema.yml` files.

## Configure a Snowflake profile

The project name in `dbt_project.yml` is `ais`, so dbt needs an `ais` profile.
For local runs, create `~/.dbt/profiles.yml` with the Snowflake account,
warehouse, role, database, schema, and key-pair configuration for your
environment. Do not commit credentials or private keys.

Airflow has a separate profile at `airflow/config/dbt/profiles.yml`. It reads
the account, user, role, warehouse, and private-key path from environment
variables and is mounted into the Airflow containers.

## Run locally

From this directory, with a compatible dbt executable and the profile
configured:

```bash
dbt deps
dbt debug
dbt source freshness
dbt build
```

To match Airflow's staged execution:

```bash
dbt build --select staging intermediate
dbt build --select marts
```

The Airflow image currently installs dbt Fusion separately from the Airflow
Python packages. Confirm that your local dbt executable supports the project
and adapter configuration before running it against Snowflake.