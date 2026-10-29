from airflow.sdk import Asset

# Updated by activity_weekly once the data is loaded into Snowflake RAW and has
# passed its data-quality checks; consumed by the dbt_transform DAG.
RAW_PORTS_ACTIVITY = Asset("ais_raw_ports_activity")
RAW_AIS = Asset("ais_raw_positions_vessels")