import json
import os
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env", override=True)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
DUMP_FILE = PROJECT_ROOT / "test_apis" / "ais_dump.jsonl"

POSITION_TYPES = {"PositionReport"}
STATIC_TYPES = {"ShipStaticData"}


def main():
    if not DUMP_FILE.exists():
        raise SystemExit(f"dump not found: {DUMP_FILE}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )

    sent = {"position_report": 0, "ship_static_data": 0}
    skipped = {}

    with open(DUMP_FILE, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue

            event = json.loads(line)
            message_type = event.get("MessageType")

            if message_type in POSITION_TYPES:
                topic = "position_report"
            elif message_type in STATIC_TYPES:
                topic = "ship_static_data"
            else:
                skipped[message_type] = skipped.get(message_type, 0) + 1
                continue

            producer.send(topic, value=event)
            sent[topic] += 1

    producer.flush()
    producer.close()

    print(f"bootstrap : {BOOTSTRAP_SERVERS}")
    print(f"source    : {DUMP_FILE}")
    print(f"sent      : position_report={sent['position_report']}  "
          f"ship_static_data={sent['ship_static_data']}")
    print(f"skipped   : {skipped}")


if __name__ == "__main__":
    main()
