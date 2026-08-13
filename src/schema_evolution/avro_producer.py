from datetime import datetime
from random import choice
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer


sr_config={
    'url':'http://localhost:8081'
}

#1.create a client
sr_client=SchemaRegistryClient(sr_config)

#2. Read the .avsc schema file
schema_path=r"D:\python\kafka-weather-pipeline\src\dlq_project\schemas\user_event_v2.avsc"
with open(schema_path,'r') as f:
    schema_str=f.read()

#3.create the avroserializer

avro_serialiser=AvroSerializer(
    schema_registry_client=sr_client,
    schema_str=schema_str,
    to_dict=lambda obj, ctx:obj
)

#4. configure serializingproducer
producer_config={
     'bootstrap.servers': 'localhost:9092',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serialiser
}

#5.Create the producer
producer=SerializingProducer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] {msg.value()}")

try:
    while True:
        for i in range(20):
            msg={
                "user_id": f'user_{i}',
                "event_type": choice(['event1','event2','event3','event4']),
                "timestamp": int(datetime.now().timestamp() * 1000),
                "source": choice(["mobile", "web"])
                }
            producer.produce(
                topic='user_events',
                key=msg['user_id'],
                value=msg,
                on_delivery=delivery_report
            )
        producer.poll(1.0)
except KeyboardInterrupt:
    print(f"Keyboard interrupt caught, exiting...")

finally:
    producer.flush()