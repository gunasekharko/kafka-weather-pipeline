from json import JSONDecodeError
from confluent_kafka import Consumer
import json
import time

DLQ_TOPIC = 'orders-dlq'

conf={
    'bootstrap.servers':'localhost:9092',
    'group.id':'dlq-inspector',
    'auto.offset.reset':'earliest',
    'enable.auto.commit': True
}

topic='orders-dlq'

consumer=Consumer(conf)
consumer.subscribe([topic])

count = 0
consecutive_empty = 0

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            consecutive_empty += 1
            if consecutive_empty >= 5:  
                break
            continue
        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue
        
        consecutive_empty = 0  
        count += 1
        payload = json.loads(msg.value().decode('utf-8'))
        
        print(" --- DLQ RECORD RECEIVED ---")
        print(f"   Original Value: {payload.get('original_value')}")
        print(f"   Error Reason  : {payload.get('error_reason')}")
        print(f"   Failed At     : {payload.get('failed_at')}")
        print(f"   Partition/Offset: [{msg.partition()}] @ {msg.offset()}\n")

except Exception as e:
    print(f"ERROR:{e}")
else:
    print(f" Total DLQ messages inspected: {count}")

finally:
    consumer.close()