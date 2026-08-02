from confluent_kafka import Producer

messages = [
    "Hello Kafka #1 - Producer is alive!",
    "Hello Kafka #2 - Sending test data",
    "Hello Kafka #3 - Weather pipeline starting",
    "Hello Kafka #4 - Temperature: 32°C",
    "Hello Kafka #5 - Humidity: 78%",
    "Hello Kafka #6 - Wind speed: 15 km/h",
    "Hello Kafka #7 - Pressure: 1013 hPa",
    "Hello Kafka #8 - Visibility: 10 km",
    "Hello Kafka #9 - Conditions: Partly cloudy",
    "Hello Kafka #10 - All messages sent successfully!"
]


conf={'bootstrap.servers': 'localhost:9092'}

producer=Producer(conf)

def deliveryreport(err,msg):
        if err:
            print(f"message delivery failed:{err}")
        else:
            print(f"message delivered to topic:{msg.topic()} {msg.partition()}")

for i in range(len(messages)):
    producer.produce('weather-events',value=messages[i],callback=deliveryreport)


producer.flush()