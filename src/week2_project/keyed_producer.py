from datetime import datetime
import json
import time
import random
from confluent_kafka import Producer
import logging


logging.basicConfig(
    format="%(levelname)s %(name)s %(asctime)s :%(message)s",
    datefmt ="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

cities=["Tokyo", "London", "NewYork", "Sydney", "Mumbai"]

producer=Producer({'bootstrap.servers':'localhost:9092'})

topic='weather-events'

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(
            f"Delivered message for Key='{msg.key().decode('utf-8')}' "
            f"to Partition [{msg.partition()}] at Offset {msg.offset()}"
        )

try:
    while True:
        selected_city=random.choice(cities)
        msg={
            'city':cities,
            "temperature":round(random.uniform(15.0,35.0),1),
            "humidity": random.randint(40,90),
            "timestamp":datetime.now().isoformat()
        }
        payload=json.dumps(msg).encode('utf-8')
        message_key=selected_city.encode('utf-8')
        producer.produce(topic=topic,key=message_key,value=payload,on_delivery=delivery_report)
        producer.poll(1.0)
        time.sleep(1)

except KeyboardInterrupt as e:
    logging.error(f"Got the User input error by clicking cntrl+c")

finally:
    logging.info("Flushing remaining messages...")
    producer.flush()