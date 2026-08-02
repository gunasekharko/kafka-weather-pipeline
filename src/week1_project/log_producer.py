import json
import random
from datetime import datetime
from confluent_kafka import Producer
import time


conf={'bootstrap.servers':'localhost:9092'}

producer=Producer(conf)

topic='application-logs'

log=['DEBUG', 'INFO', 'WARNING', 'ERROR']

IP_POOL = ['192.168.1.10', '10.0.0.5', '172.16.0.22']

def logs(err,msg):
    if err is not None:
        print(f"error {err} in message")
    else:
        print(f"message delivered to topic:{msg.topic()} partition:{msg.partition()} offset:{msg.offset()} value:{msg.value()}")

while True:
    try:
        log_entry={
            "timestamp":datetime.now().isoformat(),
            "ip":random.choice(IP_POOL),
            "level":random.choice(log),
            "message":"user action performed"
        }
        producer.produce(topic=topic,value=json.dumps(log_entry).encode('utf-8') ,callback=logs)
        producer.poll(0.5)
        time.sleep(0.5)
    except  ValueError as e:
        print(f"value error :{e}")
    finally:
        producer.flush()