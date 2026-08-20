import json
from confluent_kafka import Producer
from config import BROKER_ADDRESS, TOPIC_RAW

# Initialize producer
producer = Producer({'bootstrap.servers': BROKER_ADDRESS})

# Test payloads
payloads = [
    {"tx_id": "TX-101", "account_id": "A1", "amount_cents": 5400, "currency": "USD"},
    {"tx_id": "TX-102", "account_id": "A2", "amount_cents": -50, "currency": "USD"},
    {"tx_id": "TX-103", "account_id": "A3", "amount_cents": 1299, "currency": "USD"}
]

for payload in payloads:
    # Produce message
    producer.produce(
        topic=TOPIC_RAW,
        key=payload['tx_id'].encode('utf-8'),
        value=json.dumps(payload).encode('utf-8')
    )
    print(f"Sent: {payload}")

# Flush queue
producer.flush()
