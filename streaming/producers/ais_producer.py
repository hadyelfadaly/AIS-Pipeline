import asyncio
import json
import os
import random
import time
from collections import Counter

import websockets
from kafka import KafkaProducer

from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env", override=True)

AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

SEND_TO_KAFKA = True
_BOX_ENV = os.getenv("AIS_BOUNDING_BOX")
BOUNDING_BOX = json.loads(_BOX_ENV) if _BOX_ENV else [[[-90, -180], [90, 180]]]
MAX_MESSAGES = int(os.getenv("AIS_MAX_MESSAGES", "0")) or None

POSITION_TYPES = {"PositionReport"}
STATIC_TYPES = {"ShipStaticData"}

URI = "wss://stream.aisstream.io/v0/stream"


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
    )


async def run_connection(producer, subscription, message_counts):
    """One websocket connection. Returns True if MAX_MESSAGES reached."""
    async with websockets.connect(
        URI,
        ping_interval=20,
        ping_timeout=20,
    ) as websocket:
        await websocket.send(json.dumps(subscription))
        print("Connected to AISStream.", flush=True)

        processed_this_run = 0

        async for raw_message in websocket:
            event = json.loads(raw_message)
            message_type = event.get("MessageType", "Unknown")

            if message_type == "SubscriptionConfirmation":
                print("Subscription confirmed.", flush=True)
                continue

            if message_type in POSITION_TYPES:
                topic = "position_report"
            elif message_type in STATIC_TYPES:
                topic = "ship_static_data"
            else:
                continue

            metadata = event.get("MetaData", {})
            print(
                f"[{sum(message_counts.values()) + 1}] "
                f"Type={message_type} | "
                f"Ship={metadata.get('MMSI', 'Unknown')} | "
                f"Topic={topic}",
                flush=True,
            )

            message_counts[message_type] += 1
            processed_this_run += 1

            if SEND_TO_KAFKA:
                producer.send(topic, value=event)

            if (
                MAX_MESSAGES is not None
                and sum(message_counts.values()) >= MAX_MESSAGES
            ):
                print("\nMessage limit reached.", flush=True)
                return True

    return False  # connection ended without hitting the limit


async def main():
    if not AISSTREAM_API_KEY:
        raise ValueError("AISSTREAM_API_KEY is missing. Add it to your .env file.")

    producer = create_kafka_producer() if SEND_TO_KAFKA else None

    subscription = {
        "APIKey": AISSTREAM_API_KEY,
        "BoundingBoxes": BOUNDING_BOX,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    message_counts = Counter()
    backoff = 1

    try:
        while True:
            try:
                done = await run_connection(producer, subscription, message_counts)
                if done:
                    break
                # Server closed cleanly without us asking — reconnect
                print("[producer] connection closed by server, reconnecting...", flush=True)
            except (websockets.ConnectionClosed, asyncio.TimeoutError, OSError) as e:
                print(f"[producer] connection lost: {e}", flush=True)

            sleep_s = min(backoff + random.uniform(0, 1), 60)
            print(f"[producer] reconnecting in {sleep_s:.1f}s", flush=True)
            await asyncio.sleep(sleep_s)
            backoff = min(backoff * 2, 60)

    finally:
        if producer is not None:
            producer.flush()
            producer.close()

        print("\n========== Summary ==========")
        print(f"Processed messages: {sum(message_counts.values())}")
        for message_type, count in message_counts.items():
            print(f"{message_type}: {count}")
        print("=============================")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProducer stopped.")