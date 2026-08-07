from confluent_kafka import Consumer, KafkaError
import json
import logging
import sys
import time

# 1. Logging setup
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

conf = {
    'bootstrap.servers':'localhost:9092',
    'group.id':'weather-readers-v2',
    'auto.offset.reset':'earliest',
    'enable.auto.commit':False
}
consumer=Consumer(conf)

def on_assign(consumer, partitions):
    """Callback triggered whenever partitions are assigned to this consumer."""
    logging.info(f"PARTITIONS ASSIGNED: {[p.topic + '-' + str(p.partition) for p in partitions]}")

def on_revoke(consumer, partitions):
    """Callback triggered whenever partitions are revoked during a rebalance."""
    logging.info(f"PARTITIONS REVOKED: {[p.topic + '-' + str(p.partition) for p in partitions]}")

consumer.subscribe(['weather-events'],on_assign=on_assign,on_revoke=on_revoke)

try:
    logging.info("Starting Consumer Poll Loop... Press Ctrl+C to stop.")
    while True:
        msg=consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error is recieved in kafka")
            continue
        # TODO: Process the valid message:
        # 1. Extract key, value (JSON loads), topic, and partition
        # 2. Print formatted log showing: Consumer Group, Partition ID, Key, and Value
        key=msg.key().decode('utf-8') if msg.key() else "NO key"
        value=json.loads(msg.value().decode('utf-8'))
        partition=msg.partition()

        print(f"Partition: [{partition}] | Offset: {msg.offset()} | Key: {key} | Value: {value}")
        # Optional: Uncomment line below to test At-Least-Once delivery & crash recovery
        # if msg.offset() == 5: raise Exception("simulated crash before commit!")
        
        try:
            consumer.commit(message=msg, asynchronous=False)
            print(f"successfully committed offset:{msg.offset()}")

        except Exception as e:
            print(f"Commit failed:{e}")

except KeyboardInterrupt:
    logging.info("Keyboard interrupt received. Shutting down...")

finally:
    consumer.close()
