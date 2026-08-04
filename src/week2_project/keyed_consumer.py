from confluent_kafka import Consumer
import json
import time
import logging


logging.basicConfig(
    format="%(levelname)s %(name)s %(asctime)s :%(message)s",
    datefmt ="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)


consumer=Consumer(
    {'bootstrap.servers':'localhost:9092','group.id':'consumer','auto.offset.reset':'earliest'})

topic='weather-events'

consumer.subscribe([topic])


try:
    while True:
        msg=consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error(f"consumer error:{msg.error()}")
            continue
        key=msg.key().decode('utf-8') if msg.key() else "No key"
        payload=json.loads(msg.value().decode('utf-8'))
        partition=msg.partition()

        print(f"Partition: [{partition}] | Key: {key} | Value: {payload}")


except ValueError as e:
    logging.info('exception while parsing the message:{e}')

finally:
    logging.info('closing the consumer will last message')
    consumer.close()