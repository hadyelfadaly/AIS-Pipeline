
import json
import logging
import math
import os
import time

from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env", override=True)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"
)

POSITION_TOPIC = "position_report"
WEATHER_TOPIC = "weather_data"

MARINE_API_URL = "https://marine-api.open-meteo.com/v1/marine"
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"

# 1-degree cells: sea state barely changes over ~100 km, and worldwide AIS at 0.1
# degrees means >10k cells, far beyond the Open-Meteo free quota. Must match
# WEATHER_GRID_SIZE in spark/common.py.
GRID_SIZE = float(os.getenv("WEATHER_GRID_SIZE", "1.0"))
# Each call returns yesterday + today hourly, so a cell only needs a refresh
# every few hours, not every hour.
POLL_INTERVAL_SECONDS = int(os.getenv("WEATHER_POLL_INTERVAL_SECONDS", "21600"))
# Fetch at most this many cells per loop so the Kafka consumer keeps polling.
MAX_CELLS_PER_LOOP = int(os.getenv("WEATHER_MAX_CELLS_PER_LOOP", "20"))
RATE_LIMIT_BACKOFF_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 20
ACTIVE_GRID_HOURS = 6

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# Grid helpers
# --------------------------------------------------

def get_grid_coordinates(latitude, longitude):
    """Return the center coordinates of a 0.1-degree grid."""

    grid_lat = (
        math.floor(latitude / GRID_SIZE) * GRID_SIZE
        + GRID_SIZE / 2
    )

    grid_lon = (
        math.floor(longitude / GRID_SIZE) * GRID_SIZE
        + GRID_SIZE / 2
    )

    return round(grid_lat, 4), round(grid_lon, 4)


def get_cell_id(latitude, longitude):
    """Create a stable ID for a geographic grid."""

    return f"{latitude:.2f}_{longitude:.2f}"


def extract_position(record):
    """Extract latitude and longitude from an AIS Kafka record."""

    metadata = record.get("MetaData") or {}

    message = record.get("Message") or {}
    position = message.get("PositionReport") or {}

    latitude = metadata.get("latitude")
    longitude = metadata.get("longitude")

    if latitude is None:
        latitude = position.get("Latitude")

    if longitude is None:
        longitude = position.get("Longitude")

    if latitude is None or longitude is None:
        return None

    try:
        latitude = float(latitude)
        longitude = float(longitude)
    except (TypeError, ValueError):
        return None

    if not (-90 <= latitude <= 90):
        return None

    if not (-180 <= longitude <= 180):
        return None

    return latitude, longitude


# --------------------------------------------------
# Time helpers
# --------------------------------------------------

def parse_utc_hour(hour_string):
    """Parse an API timestamp and normalize it to UTC."""

    parsed = datetime.fromisoformat(
        hour_string.replace("Z", "+00:00")
    )

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def to_float_or_none(value):
    """Convert a numeric API value to float, preserving nulls."""

    if value is None:
        return None

    try:
        result = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(result):
        return None

    return result


# --------------------------------------------------
# Open-Meteo API
# --------------------------------------------------

def fetch_and_join_weather(cell_id, cell_lat, cell_lon):
    """
    Fetch marine and weather forecasts for one grid.

    Missing individual values are preserved as None.
    A missing weather value does not discard the whole record.
    """

    marine_params = {
        "latitude": cell_lat,
        "longitude": cell_lon,
        "hourly": "wave_height,ocean_current_velocity",
        "past_days": 1,
        "forecast_days": 2,
        "timezone": "UTC",
    }

    weather_params = {
        "latitude": cell_lat,
        "longitude": cell_lon,
        "hourly": "wind_speed_10m",
        "past_days": 1,
        "forecast_days": 2,
        "timezone": "UTC",
    }

    marine_response = requests.get(
        MARINE_API_URL,
        params=marine_params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    marine_response.raise_for_status()
    marine_data = marine_response.json()

    weather_response = requests.get(
        WEATHER_API_URL,
        params=weather_params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    weather_response.raise_for_status()
    weather_data = weather_response.json()

    marine_hourly = marine_data.get("hourly") or {}
    weather_hourly = weather_data.get("hourly") or {}

    marine_times = marine_hourly.get("time") or []
    weather_times = weather_hourly.get("time") or []

    # Find hours present in both API responses.
    common_hours = set(marine_times).intersection(weather_times)

    # Send hours up to the next scheduled refresh: positions arriving before
    # this cell is fetched again then still find their (cell, hour). Forecast
    # values are replaced by newer polls, since the Spark cache keeps the
    # latest polled_at for each (cell, hour).
    horizon = datetime.now(timezone.utc).replace(
        minute=0, second=0, microsecond=0,
    ) + timedelta(seconds=POLL_INTERVAL_SECONDS)

    eligible_hours = []

    for hour in common_hours:
        try:
            if parse_utc_hour(hour) <= horizon:
                eligible_hours.append(hour)
        except (TypeError, ValueError):
            logger.warning("Invalid API timestamp for grid %s: %s", cell_id, hour)

    wave_values = marine_hourly.get("wave_height") or []
    current_values = marine_hourly.get("ocean_current_velocity") or []
    wind_values = weather_hourly.get("wind_speed_10m") or []

    records = []

    # Emit every eligible hour the API returned, not just the newest one.
    # The API already sends 48 hours per call, so this costs no extra requests,
    # and it lets positions from earlier hours match on (cell_id, hour).
    for hour in sorted(eligible_hours, key=parse_utc_hour):

        marine_index = marine_times.index(hour)
        weather_index = weather_times.index(hour)

        wave_height = to_float_or_none(
            wave_values[marine_index] if marine_index < len(wave_values) else None
        )
        current_velocity = to_float_or_none(
            current_values[marine_index] if marine_index < len(current_values) else None
        )
        wind_speed = to_float_or_none(
            wind_values[weather_index] if weather_index < len(wind_values) else None
        )

        records.append({
            "cell_id": cell_id,
            "cell_lat": cell_lat,
            "cell_lon": cell_lon,
            "hour": hour,
            "wave_height_m": wave_height,
            "current_velocity_kmh": current_velocity,
            "wind_speed_kmh": wind_speed,
            "polled_at": int(time.time()),
        })

    return records
# --------------------------------------------------

def create_kafka_clients():
    consumer = KafkaConsumer(
        POSITION_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="weather-grid-discovery",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value
        ).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5,
    )

    return consumer, producer


# --------------------------------------------------
# Main pipeline
# --------------------------------------------------

def main():
    consumer = None
    producer = None

    # cell_id -> grid coordinates and last observed time
    active_grids = {}

    # cell_id -> last successful (or failed) fetch time
    last_fetched = {}
    paused_until = 0

    try:
        consumer, producer = create_kafka_clients()

        logger.info(
            "Weather Producer started. Listening to topic: %s",
            POSITION_TOPIC,
        )

        while True:
            # Discover grids from incoming vessel positions.
            records = consumer.poll(
                timeout_ms=1000,
                max_records=500,
            )

            now = time.time()

            for messages in records.values():
                for message in messages:
                    coordinates = extract_position(message.value)

                    if coordinates is None:
                        continue

                    latitude, longitude = coordinates

                    cell_lat, cell_lon = get_grid_coordinates(
                        latitude,
                        longitude,
                    )

                    cell_id = get_cell_id(cell_lat, cell_lon)

                    is_new_grid = cell_id not in active_grids

                    active_grids[cell_id] = {
                        "cell_lat": cell_lat,
                        "cell_lon": cell_lon,
                        "last_seen": now,
                    }

                    if is_new_grid:
                        logger.info(
                            "Discovered grid: %s (%.4f, %.4f)",
                            cell_id,
                            cell_lat,
                            cell_lon,
                        )

            # Remove grids with no vessel activity for six hours.
            expired_grids = [
                cell_id
                for cell_id, grid in active_grids.items()
                if now - grid["last_seen"]
                > ACTIVE_GRID_HOURS * 3600
            ]

            for cell_id in expired_grids:
                del active_grids[cell_id]
                last_fetched.pop(cell_id, None)
                logger.info(
                    "Removed inactive grid: %s",
                    cell_id,
                )

            # Fetch new cells right away and refresh old ones every
            # POLL_INTERVAL_SECONDS, a few cells per loop.
            due = [
                cell_id
                for cell_id in active_grids
                if now - last_fetched.get(cell_id, 0) >= POLL_INTERVAL_SECONDS
            ]

            if due and now >= paused_until:
                sent_cells = 0

                for cell_id in due[:MAX_CELLS_PER_LOOP]:
                    grid = active_grids[cell_id]
                    try:
                        weather_records = fetch_and_join_weather(
                            cell_id=cell_id,
                            cell_lat=grid["cell_lat"],
                            cell_lon=grid["cell_lon"],
                        )

                        for weather_record in weather_records:
                            producer.send(
                                WEATHER_TOPIC,
                                key=cell_id,
                                value=weather_record,
                            )

                        last_fetched[cell_id] = time.time()
                        sent_cells += 1

                    except requests.HTTPError as error:
                        status = getattr(error.response, "status_code", None)
                        if status == 429:
                            logger.warning(
                                "Open-Meteo rate limit hit, pausing %d s",
                                RATE_LIMIT_BACKOFF_SECONDS,
                            )
                            paused_until = time.time() + RATE_LIMIT_BACKOFF_SECONDS
                            break
                        logger.error("HTTP error for grid %s: %s", cell_id, error)
                        last_fetched[cell_id] = time.time()

                    except (
                        requests.RequestException,
                        ValueError,
                        RuntimeError,
                        KafkaError,
                        TimeoutError,
                    ) as error:
                        logger.error(
                            "Failed to process grid %s: %s",
                            cell_id,
                            error,
                        )
                        # retry this cell on the next refresh, not every loop
                        last_fetched[cell_id] = time.time()

                    except Exception:
                        logger.exception(
                            "Unexpected error for grid %s",
                            cell_id,
                        )
                        last_fetched[cell_id] = time.time()

                if sent_cells:
                    producer.flush()
                    logger.info(
                        "Sent weather for %d cell(s); %d still due, %d active",
                        sent_cells,
                        max(len(due) - sent_cells, 0),
                        len(active_grids),
                    )

    except KeyboardInterrupt:
        logger.info("Stopping Weather Producer...")

    finally:
        if consumer is not None:
            consumer.close()

        if producer is not None:
            producer.flush()
            producer.close()

        logger.info("Weather Producer stopped.")


if __name__ == "__main__":
    main()
