import json
from confluent_kafka import Consumer


conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id':          'humidity',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = 'weather-events'
consumer.subscribe([topic])

print(f" Subscribed to '{topic}'. Waiting for messages... (Ctrl+C to stop)\n")

try:
    while True:
        msg=consumer.poll(1.0)
        if msg is None:
            print(f"No messages not there in the queue")
            continue
        elif msg.error():
            print(f"error in the kafka messages")
            continue
        else:
            data=json.loads(msg.value().decode('utf-8'))
            print(f"[Partition:{msg.partition()} | Offset:{msg.offset()}] City:{data['city']} | Temp:{data['temperature']} | Humidity:{data['humidity']}")

except KeyboardInterrupt:
    print(f"user stopped using cntrl+c")

except (json.JSONDecodeError,UnicodeDecodeError) as e:
    print(f"Faile to decode message payload:{e}")

finally:
    consumer.close()