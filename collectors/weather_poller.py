import json
import time

import requests

from collectors import config
from collectors.kafka_utils import build_producer, send


def _normalise_hour(stamp):
    return stamp.replace("T", " ") + ":00"


def _as_list(payload):
    return payload if isinstance(payload, list) else [payload]


def fetch_weather(cells):
    lats = ",".join(f"{lat:.4f}" for _, lat, _ in cells)
    lons = ",".join(f"{lon:.4f}" for _, _, lon in cells)

    marine = requests.get(
        config.MARINE_API,
        params={
            "latitude": lats,
            "longitude": lons,
            "hourly": "wave_height,ocean_current_velocity",
            "forecast_days": 2,
            "timezone": "UTC",
        },
        timeout=90,
    )
    marine.raise_for_status()

    wind = requests.get(
        config.FORECAST_API,
        params={
            "latitude": lats,
            "longitude": lons,
            "hourly": "wind_speed_10m",
            "forecast_days": 2,
            "timezone": "UTC",
        },
        timeout=90,
    )
    wind.raise_for_status()

    return _as_list(marine.json()), _as_list(wind.json())


def emit(producer, cells, marine_payloads, wind_payloads):
    sent = 0
    for index, (cell, lat_c, lon_c) in enumerate(cells):
        marine = marine_payloads[index]
        wind = wind_payloads[index]

        times = marine.get("hourly", {}).get("time", [])
        waves = marine.get("hourly", {}).get("wave_height", [])
        currents = marine.get("hourly", {}).get("ocean_current_velocity", [])
        winds = wind.get("hourly", {}).get("wind_speed_10m", [])

        for position, stamp in enumerate(times):
            record = {
                "cell_id": cell,
                "cell_lat": round(lat_c, 4),
                "cell_lon": round(lon_c, 4),
                "hour": _normalise_hour(stamp),
                "wave_height_m": waves[position] if position < len(waves) else None,
                "current_velocity_kmh": currents[position] if position < len(currents) else None,
                "wind_speed_kmh": winds[position] if position < len(winds) else None,
                "polled_at": int(time.time()),
            }
            send(producer, config.TOPIC_WEATHER, cell, json.dumps(record))
            sent += 1
    return sent


def main():
    cells = config.grid_cells()
    print(f"polling {len(cells)} grid cells every {config.WEATHER_POLL_SECONDS}s", flush=True)

    producer = build_producer()
    try:
        while True:
            started = time.time()
            try:
                marine_payloads, wind_payloads = fetch_weather(cells)
                count = emit(producer, cells, marine_payloads, wind_payloads)
                producer.flush()
                print(f"published {count} weather rows in {time.time() - started:.1f}s", flush=True)
            except Exception as exc:
                print(f"[weather] poll failed: {exc}", flush=True)

            time.sleep(config.WEATHER_POLL_SECONDS)
    except KeyboardInterrupt:
        print("weather poller stopped", flush=True)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
