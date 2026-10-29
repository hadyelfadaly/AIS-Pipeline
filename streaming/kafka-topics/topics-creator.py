
import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Kafka connection settings
bootstrap_servers = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "127.0.0.1:9092"
)

# Topics configuration
topics = [
    NewTopic(
        name="position_report",
        num_partitions=3,
        replication_factor=1
    ),
    NewTopic(
        name="ship_static_data",
        num_partitions=1,
        replication_factor=1
    ),
    NewTopic(
        name="weather_data",
        num_partitions=1,
        replication_factor=1
    ),
]


def main():
    admin = None

    try:
        print(f"Connecting to Kafka at {bootstrap_servers}...")

        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="ais-topic-creator",
            request_timeout_ms=30000
        )

        print("Connected successfully!")

        existing_topics = set(admin.list_topics())

        topics_to_create = []

        for topic in topics:
            if topic.name in existing_topics:
                print(f"[EXISTS] Topic '{topic.name}' already exists.")
            else:
                topics_to_create.append(topic)

        if not topics_to_create:
            print("All required topics already exist.")
            return

        results = admin.create_topics(
            new_topics=topics_to_create,
            validate_only=False
        )

        for topic_name, result in results.topic_errors.items():
            if result.error_code == 0:
                print(f"[CREATED] Topic '{topic_name}' created successfully.")
            else:
                print(
                    f"[ERROR] Could not create '{topic_name}': "
                    f"{result.error_message}"
                )

        print("\nTopic creation process finished.")

    except Exception as error:
        print(f"[ERROR] {type(error).__name__}: {error}")

    finally:
        if admin is not None:
            admin.close()


if __name__ == "__main__":
    main()
