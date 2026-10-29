import asyncio
import json
from collections import Counter

from producers import ais_producer


class FakeWebSocket:
    def __init__(self, messages):
        self.messages = iter(messages)
        self.sent = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return False

    async def send(self, message):
        self.sent.append(message)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.messages)
        except StopIteration:
            raise StopAsyncIteration


class FakeKafkaProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, value):
        self.sent.append((topic, value))


def test_run_connection_routes_supported_messages_and_ignores_others(monkeypatch):
    events = [
        {"MessageType": "SubscriptionConfirmation"},
        {
            "MessageType": "PositionReport",
            "MetaData": {"MMSI": 123},
            "Message": {"PositionReport": {"Sog": 12.5}},
        },
        {
            "MessageType": "ShipStaticData",
            "MetaData": {"MMSI": 123},
            "Message": {"ShipStaticData": {"Name": "Test Vessel"}},
        },
        {"MessageType": "UnsupportedMessage"},
    ]
    websocket = FakeWebSocket(json.dumps(event) for event in events)
    producer = FakeKafkaProducer()
    monkeypatch.setattr(
        ais_producer.websockets,
        "connect",
        lambda *args, **kwargs: websocket,
    )
    counts = Counter()
    subscription = {
        "APIKey": "test-key",
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    completed = asyncio.run(
        ais_producer.run_connection(producer, subscription, counts)
    )

    assert completed is False
    assert json.loads(websocket.sent[0]) == subscription
    assert [topic for topic, _ in producer.sent] == [
        "position_report",
        "ship_static_data",
    ]
    assert counts == {
        "PositionReport": 1,
        "ShipStaticData": 1,
    }


def test_run_connection_stops_at_message_limit(monkeypatch):
    events = [
        {"MessageType": "PositionReport", "MetaData": {"MMSI": 1}},
        {"MessageType": "ShipStaticData", "MetaData": {"MMSI": 2}},
        {"MessageType": "PositionReport", "MetaData": {"MMSI": 3}},
    ]
    websocket = FakeWebSocket(json.dumps(event) for event in events)
    producer = FakeKafkaProducer()
    monkeypatch.setattr(
        ais_producer.websockets,
        "connect",
        lambda *args, **kwargs: websocket,
    )
    monkeypatch.setattr(ais_producer, "MAX_MESSAGES", 2)
    counts = Counter()

    completed = asyncio.run(
        ais_producer.run_connection(producer, {}, counts)
    )

    assert completed is True
    assert len(producer.sent) == 2
    assert sum(counts.values()) == 2
