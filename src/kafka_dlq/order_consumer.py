from json import JSONDecodeError
from confluent_kafka import Consumer,Producer
import json
import time

dlq_producer = Producer({'bootstrap.servers': 'localhost:9092'})
DLQ_TOPIC = 'orders-dlq'

conf={
    'bootstrap.servers':'localhost:9092',
    'group.id':'order_processing',
    'auto.offset.reset':'earliest',
    'enable.auto.commit': False
}

topic='orders'

consumer=Consumer(conf)
consumer.subscribe([topic])

def validate_order(payload):
    if not payload.get('order_id') or payload.get('amount') is None or payload.get('amount') <= 0:
        raise ValueError("order_id or amount is missing or invalid in payload")
    

def send_to_dlq(msg,err):
    raw_str=msg.value().decode('utf-8',errors='replace') if msg else ""
    dlq_payload = {
        "original_value": raw_str,
        "error_reason": str(err),
        "failed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())  # UTC timestamp
    }
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=msg.key(),  # Retain original customer key for partition routing
        value=json.dumps(dlq_payload).encode('utf-8')
    )
    # Flush to ensure message is delivered to Kafka before returning
    dlq_producer.flush()
    print(f"Successfully forwarded message to {DLQ_TOPIC}")

try:
    while True:
        msg=consumer.poll(1.0)
        if msg is None:
            continue
        elif msg.error():
            print(f"got the error in kafka messages")
        else:
            try:
                payload=json.loads(msg.value().decode('utf-8'))
                print(payload)
                validate_order(payload)
                print(f"Successfully taking message")
                time.sleep(1)
            except(JSONDecodeError,KeyError,ValueError) as err:
                send_to_dlq(msg,str(err))
                print(f'routed to DLQ')
            finally:
                consumer.commit(message=msg,asynchronous=False)
except Exception as e:
    print(f'getting error:{e}')

finally:
    consumer.close()