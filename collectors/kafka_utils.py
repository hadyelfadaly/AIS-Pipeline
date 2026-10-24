from kafka import KafkaProducer

from collectors import config


def build_producer():
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        acks="all",
        retries=5,
        linger_ms=50,
        max_block_ms=20000,
    )


def send(producer, topic, key, value):
    try:
        producer.send(topic, key=str(key), value=value)
    except Exception as exc:
        dead_letter(producer, key, value, f"send to {topic} failed: {exc}")


def dead_letter(producer, key, value, reason):
    print(f"[dlq] {reason}", flush=True)
    try:
        producer.send(config.TOPIC_DLQ, key=str(key), value=value)
    except Exception as exc:
        print(f"[dlq] could not write to dlq: {exc}", flush=True)
