from datetime import timezone,datetime
from confluent_kafka import Producer
import json
import time
import random
from config import BROKER_ADDRESS , TOPIC_PROCESSED , TOPIC_RAW,CITIES

producer=Producer(
    {'bootstrap.servers':BROKER_ADDRESS}
)

def ondeliverty(err,msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered topic:{msg.topic()} partition:{msg.partition()} message:{msg.value()}")

try:
    while True:
        for city in CITIES:
            record={
                'station_id':f'{city[:3].upper()}-01',
                'city'      :city ,
                'temperature': random.uniform(15.0,42.0),
                'humidity'   : random.randint(15,42),
                'timestamp'  : datetime.now(timezone.utc).isoformat()
            }
            if random.random()<0.1:
                corruption_type = random.choice(['missing_field', 'bad_temp', 'bad_format'])
                if corruption_type == 'missing_field':
                   del record['temperature']        
                elif corruption_type == 'bad_temp':
                   record['temperature'] = random.uniform(100, 200) 
                elif corruption_type == 'bad_format':
                   record['humidity'] = 'NOT_A_NUMBER' 
                

            producer.produce(topic=TOPIC_RAW,
            key=record['city'].encode('UTF-8'),
            value=json.dumps(record).encode('UTF-8'),callback=ondeliverty)
            producer.poll()
            time.sleep(1)
except KeyboardInterrupt as e:
    print(f"We got user interrupt:{e}")

finally:
    producer.flush()