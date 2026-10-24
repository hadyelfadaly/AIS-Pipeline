# AIS & Port Data Pipeline

Maritime intelligence platform. The question it answers: **why do ships arrive late, and is it
the weather or something else?**

Live vessel traffic (AISStream), marine weather (Open-Meteo), port activity (IMF PortWatch) and
trade flows (UN Comtrade) are combined into a hot path (real-time) and a cold path (analytics).

---

## Status

| Layer | Owner area | State |
|---|---|---|
| Data sources + ingestion (AIS, weather) | streaming team | done |
| Kafka topics + producers | streaming team | done |
| Bronze + Silver in Spark Structured Streaming | streaming team | done |
| Batch sources (PortWatch, UN Comtrade) | streaming team | not started |
| Snowflake + dbt star schema (cold path) | analytics team | not started |
| Grafana / Power BI | serving team | not started |
| Airflow, Prometheus, EC2 | platform | not started |

---

## Architecture (streaming half)

```
AISStream (WebSocket)  --+
                         +--> collectors --> Kafka --> Spark Structured Streaming --> lake
Open-Meteo (HTTP)      --+                        |
                                                  |
                                             ais.dlq (bad messages)

lake/
  bronze/   raw json, partitioned by ingest date
  silver/   clean parquet, partitioned by year/month/day
```

### Kafka topics

| Topic | Partitions | Key | Payload |
|---|---|---|---|
| `ais.raw` | 3 | MMSI | raw `PositionReport` JSON |
| `ais.static` | 3 | MMSI | raw `ShipStaticData` JSON (name, type, destination, ETA) |
| `weather.raw` | 2 | grid cell id | hourly weather per sea cell |
| `ais.dlq` | 1 | MMSI | messages that failed validation |

Partitioning is keyed on MMSI so every position of one vessel lands in the same partition, in
order. That is what lets the stream processor follow a single ship's timeline without shuffling.

### Lake layout

```
data/lake/                      (LAKE_ROOT, becomes s3://maritime-lake/ later)
  bronze/
    ais_positions/ingest_date=YYYY-MM-DD/
    ais_static/ingest_date=YYYY-MM-DD/
    weather/ingest_date=YYYY-MM-DD/
  weather_hourly/               latest weather per cell per hour
  silver/
    live_telemetry/year=YYYY/month=MM/day=DD/
```

`silver/live_telemetry` is the handoff contract for the analytics and serving teams. Columns:

```
mmsi, ship_name, ship_type, navigational_status, latitude, longitude,
speed_over_ground_knots, course_over_ground, destination_port, eta_raw,
wind_speed, wave_height_meters, ocean_current_velocity, timestamp,
year, month, day
```

Partitioned by `year/month/day` so a live dashboard only scans today's files.

---

## Running it

### 1. Prerequisites

- Docker + Docker Compose
- Python 3.12 with `venv`
- Apache Spark 3.5.x (for the stream processor) — Java 17 or 21

### 2. Configure

```bash
cp .env.example .env
# then put your real AISStream key into .env
```

Get a free key at <https://aisstream.io/account>. Never commit `.env`.

### 3. Start Kafka and the collectors

```bash
docker compose up -d
docker compose ps
```

Three containers start: `kafka`, `ais-collector`, `weather-poller`.

Kafka is reachable two ways:

- `localhost:9092` from your machine
- `kafka:29092` from inside the Docker network

### 4. Create the topics (first time only, after Kafka is up)

```bash
KT="docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"
$KT --create --topic ais.raw     --partitions 3 --replication-factor 1
$KT --create --topic ais.static  --partitions 3 --replication-factor 1
$KT --create --topic weather.raw --partitions 2 --replication-factor 1
$KT --create --topic ais.dlq     --partitions 1 --replication-factor 1
$KT --list
```

### 5. Run the stream processor

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
  streaming/build_lake.py
```

Leave it running. It writes bronze, the weather table, and silver continuously.

### 6. Read the lake with DuckDB (no Spark needed)

```bash
python - <<'EOF'
import duckdb
c = duckdb.connect()
print(c.sql("select count(*), count(wind_speed) from 'data/lake/silver/live_telemetry/**/*.parquet'").fetchall())
EOF
```

---

## Collector behaviour

**`collectors/ais_collector.py`**

- Opens `wss://stream.aisstream.io/v0/stream` and subscribes to the Suez bounding box
- Routes `PositionReport` to `ais.raw`, static reports to `ais.static`, keyed by MMSI
- Reconnects with exponential backoff + jitter (1s to 60s)
- Sends unparseable or unkeyable messages to `ais.dlq`
- Flushes and closes the producer on SIGINT / SIGTERM

**`collectors/weather_poller.py`**

- Covers the same box as 35 cells of 0.5 degrees, requests 48 hours of hourly data per cell
- Normalises hour to `YYYY-MM-DD HH:00:00` so it joins to AIS event time directly
- Re-polls every `WEATHER_POLL_SECONDS` (default 900)

**`streaming/build_lake.py`**

- Runs three streaming queries in one Spark application
- `ais.static` to an in-memory vessel cache (name, type, destination, ETA)
- `weather.raw` to `weather_hourly` parquet, deduplicated by latest `polled_at`
- `ais.raw` parsed, enriched with the static cache and the weather table, written to Silver

### Known limitations

- Static vessel data is cached **in the Spark driver's memory**. Fine for one driver, lost on
  restart, and not shared between workers. A production version would use a stateful
  stream-stream join or a lookup store such as Redis.
- Nulls do happen: grid cells over land have no marine data, so `wave_height_meters` can be
  null for a row. That is data reality, not a bug.
- `ShipStaticData` arrives rarely, so early rows in a fresh deployment may have `ship_type`,
  `destination_port` and `eta_raw` empty until that vessel's static message has been seen.
- The Python collectors are containerised; Spark runs via `spark-submit` on the host. Moving the
  Spark driver into Compose is the next infrastructure step.

---

## Repository layout

```
collectors/
  config.py            settings, bounding box, grid helpers
  kafka_utils.py       producer factory, dead-letter helper
  ais_collector.py     AISStream -> Kafka
  weather_poller.py    Open-Meteo -> Kafka
streaming/
  build_lake.py        Spark Structured Streaming: bronze + silver
docker-compose.yml     Kafka + collectors
Dockerfile             collector image
requirements.txt
```
