
import json
import logging
import math
import os
import time

from datetime import datetime, timezone
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

GRID_SIZE = 0.1
POLL_INTERVAL_SECONDS = int(os.getenv("WEATHER_POLL_INTERVAL_SECONDS", "3600"))
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
        "forecast_days": 2,
        "timezone": "UTC",
    }

    weather_params = {
        "latitude": cell_lat,
        "longitude": cell_lon,
        "hourly": "wind_speed_10m",
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

    # Do not use a future hour for the current update.
    current_hour = datetime.now(timezone.utc).replace(
        minute=0,
        second=0,
        microsecond=0,
    )

    eligible_hours = []

    for hour in common_hours:
        try:
            if parse_utc_hour(hour) <= current_hour:
                eligible_hours.append(hour)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid API timestamp for grid %s: %s",
                cell_id,
                hour,
            )

    if not eligible_hours:
        raise RuntimeError(
            f"No common eligible hour returned by APIs for grid {cell_id}"
        )

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

    next_weather_update = 0

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
                logger.info(
                    "Removed inactive grid: %s",
                    cell_id,
                )

            # Run the first update as soon as a grid is available,
            # then repeat at the configured polling interval.
            if active_grids and now >= next_weather_update:
                logger.info(
                    "Starting weather update for %d active grid(s)",
                    len(active_grids),
                )

                for cell_id, grid in list(active_grids.items()):
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

                        logger.info(
                            "Sent %d weather hour(s) for cell=%s",
                            len(weather_records),
                            cell_id,
                        )

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

                    except Exception:
                        logger.exception(
                            "Unexpected error for grid %s",
                            cell_id,
                        )

                # Flush ensures queued Kafka records are delivered.
                producer.flush()

                next_weather_update = (
                    time.time() + POLL_INTERVAL_SECONDS
                )

                logger.info(
                    "Weather update completed. Next update in %d seconds.",
                    POLL_INTERVAL_SECONDS,
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
