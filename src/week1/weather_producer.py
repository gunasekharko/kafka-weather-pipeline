import time
from datetime import datetime
from confluent_kafka import Producer
import random
import json


conf={"bootstrap.servers":"localhost:9092"}

producer=Producer(conf)

def deliveryreport(err,msg):
        if err:
            print(f"message delivery failed:{err}")
        else:
            print(f"message delivered to topic:{msg.topic()} {msg.partition()} {msg.value()}")

x=[]

for i in range(1000):
    data={
        "city": f"city_{i}",
        "temperature":round(random.uniform(15.0,45.0),1),
        "humidity":random.randint(10,100),
        "timestamp": datetime.now().isoformat()
    }
    producer.produce('weather-events',value=json.dumps(data).encode('utf-8'),callback=deliveryreport)
    producer.poll(0)
    time.sleep(1)

    


producer.flush()



