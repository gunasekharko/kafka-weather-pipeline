from confluent_kafka import Consumer


conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id':          'basic',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = 'weather-events'
consumer.subscribe([topic])

print(f" Subscribed to '{topic}'. Waiting for messages... (Ctrl+C to stop)\n")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue  # No messages right now — silently wait
        elif msg.error():
            print(f"Error occurred while fetching data:{msg.error()}")
            continue
        else:
            print(
                f"[{msg.partition()}:{msg.offset()}] "
                f"{msg.value().decode('utf-8')}"
            )
except KeyboardInterrupt:
    print('\nshutting down')

finally:
    consumer.close()
    print("Consumer closed.")
