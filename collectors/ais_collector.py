import asyncio
import json
import random
import signal

import websockets

from collectors import config
from collectors.kafka_utils import build_producer, dead_letter, send

STOPPING = False


def _stop(signum, frame):
    global STOPPING
    STOPPING = True


def route(producer, frame):
    try:
        message = json.loads(frame)
    except json.JSONDecodeError:
        dead_letter(producer, "", frame, "message is not valid json")
        return

    message_type = message.get("MessageType")

    if message_type == "PositionReport":
        topic = config.TOPIC_AIS
    elif message_type in config.STATIC_TYPES:
        topic = config.TOPIC_STATIC
    else:
        return

    mmsi = (message.get("MetaData") or {}).get("MMSI")
    if not mmsi:
        dead_letter(producer, "", frame, f"{message_type} arrived without an MMSI")
        return

    send(producer, topic, mmsi, frame)


async def consume(producer):
    attempt = 0
    while not STOPPING:
        try:
            async with websockets.connect(
                config.STREAM_URL, ping_interval=20, close_timeout=5
            ) as socket:
                await socket.send(json.dumps({
                    "APIKey": config.AISSTREAM_API_KEY,
                    "BoundingBoxes": config.SUEZ_BOX,
                    "FilterMessageTypes": [
                        "PositionReport", "ShipStaticData", "StaticDataReport"
                    ],
                }))
                print("connected to AISStream", flush=True)
                attempt = 0

                async for frame in socket:
                    if STOPPING:
                        break
                    if isinstance(frame, bytes):
                        frame = frame.decode("utf-8")
                    route(producer, frame)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            delay = min(60.0, 2 ** min(attempt, 6)) + random.uniform(0, 1)
            print(f"[reconnect] {exc} -> retry in {delay:.1f}s", flush=True)
            await asyncio.sleep(delay)


def main():
    if not config.AISSTREAM_API_KEY:
        raise SystemExit("AISSTREAM_API_KEY is not set - add it to .env")

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    producer = build_producer()
    try:
        asyncio.run(consume(producer))
    except KeyboardInterrupt:
        pass
    finally:
        print("flushing producer...", flush=True)
        producer.flush()
        producer.close()
        print("collector stopped", flush=True)


if __name__ == "__main__":
    main()
