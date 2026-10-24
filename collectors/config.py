import os

from dotenv import load_dotenv

load_dotenv()

AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_BOOTSTRAP_EXTERNAL = os.getenv("KAFKA_BOOTSTRAP_EXTERNAL", "localhost:9092")
LAKE_ROOT = os.getenv("LAKE_ROOT", "data/lake")
WEATHER_POLL_SECONDS = int(os.getenv("WEATHER_POLL_SECONDS", "900"))

STREAM_URL = "wss://stream.aisstream.io/v0/stream"
MARINE_API = "https://marine-api.open-meteo.com/v1/marine"
FORECAST_API = "https://api.open-meteo.com/v1/forecast"

SUEZ_BOX = [[[29.0, 31.5], [32.5, 34.0]]]
BOX_SOUTH, BOX_WEST = 29.0, 31.5
BOX_NORTH, BOX_EAST = 32.5, 34.0
GRID_DEG = 0.5

TOPIC_AIS = "ais.raw"
TOPIC_STATIC = "ais.static"
TOPIC_WEATHER = "weather.raw"
TOPIC_DLQ = "ais.dlq"

STATIC_TYPES = {"ShipStaticData", "StaticDataReport"}


def cell_id(lat, lon):
    return f"{int(lat // GRID_DEG)}:{int(lon // GRID_DEG)}"


def grid_cells():
    cells = []
    lat = BOX_SOUTH
    while lat < BOX_NORTH:
        lon = BOX_WEST
        while lon < BOX_EAST:
            lat_c = lat + GRID_DEG / 2
            lon_c = lon + GRID_DEG / 2
            cells.append((cell_id(lat_c, lon_c), lat_c, lon_c))
            lon += GRID_DEG
        lat += GRID_DEG
    return cells
