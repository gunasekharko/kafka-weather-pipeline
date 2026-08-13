from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer

# 1. Configure the Schema Registry Client
sr_config = {
    'url': 'http://localhost:8081'
}
sr_client = SchemaRegistryClient(sr_config)

# 2. Read the .avsc schema file for the reader schema
schema_path=r"D:\python\kafka-weather-pipeline\src\dlq_project\schemas\user_event_v2.avsc"
with open(schema_path, 'r') as f:
    schema_str = f.read()

# 3. Create the Avro Deserializer
avro_deserializer = AvroDeserializer(
    schema_registry_client=sr_client,
    schema_str=schema_str
)

# 4. Configure the DeserializingConsumer
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'avro-user-group',
    'auto.offset.reset': 'earliest',
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': avro_deserializer
}

consumer = DeserializingConsumer(consumer_config)

# 5. Subscribe to the user_events topic
consumer.subscribe(['user_events'])

print("Starting consumer... Press Ctrl+C to exit.")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # The key and value are automatically deserialized!
        print(f"Successfully consumed message: Key={msg.key()} Value={msg.value()}")

except KeyboardInterrupt:
    print("Exiting...")
finally:
    consumer.close()
