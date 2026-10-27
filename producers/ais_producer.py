import asyncio
import json
import os
from collections import Counter

import websockets
# from dotenv import load_dotenv
from kafka import KafkaProducer

# load_dotenv()



from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE, override=True)



# Configuration
AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "127.0.0.1:9092"
)

print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

# False: inspect messages without sending them to Kafka
# True: send messages to Kafka
SEND_TO_KAFKA = True

# Global bounding box: [[south-west], [north-east]]
BOUNDING_BOX = [[[-90, -180], [90, 180]]]

# Stop after processing this many supported messages.
# Set to None for continuous streaming.
MAX_MESSAGES = 50

# Message types routed to Kafka topics
POSITION_TYPES = {
    "PositionReport",
    # "StandardClassBPositionReport"
}

STATIC_TYPES = {
    "ShipStaticData",
    # "StaticDataReport"
}


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value
        ).encode("utf-8"),
        acks="all",
        retries=5
    )


async def main():
    if not AISSTREAM_API_KEY:
        raise ValueError(
            "AISSTREAM_API_KEY is missing. "
            "Add it to your .env file."
        )

    producer = create_kafka_producer() if SEND_TO_KAFKA else None

    subscription = {
        "APIKey": AISSTREAM_API_KEY,
        "BoundingBoxes": BOUNDING_BOX,
        "FilterMessageTypes": [
            "PositionReport",
            "ShipStaticData",
        ]
    }

    message_counts = Counter()
    processed_messages = 0

    uri = "wss://stream.aisstream.io/v0/stream"

    try:
        async with websockets.connect(
            uri,
            ping_interval=20,
            ping_timeout=20
        ) as websocket:

            await websocket.send(json.dumps(subscription))

            print("Connected to AISStream.")
            print(f"Bounding box: GLOBAL")
            print(f"Send to Kafka: {SEND_TO_KAFKA}")
            print(f"Maximum messages: {MAX_MESSAGES}")
            print("Waiting for messages...\n")

            async for raw_message in websocket:
                event = json.loads(raw_message)

                message_type = event.get(
                    "MessageType",
                    "Unknown"
                )

                if message_type == "SubscriptionConfirmation":
                    print("Subscription confirmed.")
                    continue

                # Determine the destination topic
                if message_type in POSITION_TYPES:
                    topic = "position_report"

                elif message_type in STATIC_TYPES:
                    topic = "ship_static_data"

                else:
                    print(
                        f"Unsupported message type: {message_type}"
                    )
                    continue

                processed_messages += 1
                message_counts[message_type] += 1

                # Print a short summary instead of the full JSON
                metadata = event.get("MetaData", {})

                print(
                    
                    f"[{processed_messages}] "
                    # f"Message {event} "
                    f"Type={message_type} | "
                    f"Ship={metadata.get('MMSI', 'Unknown')} | "
                    f"Topic={topic if SEND_TO_KAFKA else 'Inspection only'}"
                )

                # Send the original event to Kafka
                if SEND_TO_KAFKA:
                    producer.send(topic, value=event)

                # Stop after reaching the test limit
                if (
                    MAX_MESSAGES is not None
                    and processed_messages >= MAX_MESSAGES
                ):
                    print("\nMessage limit reached.")
                    break

    finally:
        if producer is not None:
            producer.flush()
            producer.close()

        print("\n========== Summary ==========")
        print(f"Processed messages: {processed_messages}")

        for message_type, count in message_counts.items():
            print(f"{message_type}: {count}")

        print("=============================")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProducer stopped.")
