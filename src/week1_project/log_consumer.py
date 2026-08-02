import logging
from ast import Continue
import logging
from confluent_kafka import Consumer
import json
import time
import sys

conf={
    'bootstrap.servers':'localhost:9092',
    'group.id':'week1',
    'auto.offset.reset':'earliest'
    }

logging.basicConfig(
    format="%(levelname)s %(name)s %(asctime)s :%(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger=logging.getLogger('datapipeline')
logger.setLevel(level="INFO")

# 1. Terminal Handler (prints to console screen)
customer_handler = logging.StreamHandler(sys.stdout)
customer_handler.setLevel(logging.INFO)
customer_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
customer_handler.setFormatter(customer_formatter)
logger.addHandler(customer_handler)

# 2. File Handler (writes ERROR logs to errors.log file on disk)
file_handler = logging.FileHandler('errors.log')
file_handler.setLevel(logging.ERROR)
file_handler.setFormatter(customer_formatter)
logger.addHandler(file_handler)




topic='application-logs'
consumer=Consumer(conf)
consumer.subscribe([topic])

print(f" Subscribed to '{topic}'. Waiting for messages... (Ctrl+C to stop)\n")

error_count=0
total_count=0

try:
    while True:
        msg=consumer.poll(1.0)
        if msg is None:
            continue
        elif msg.error():
            print(f"Error occurred while fetching data:{msg.error()}")
            continue
        else:
            data=json.loads(msg.value().decode('utf-8'))['level']
            if(data=='ERROR'):
                error_count=error_count +1
                logger.error(f"error_count={error_count} in log file with message:{msg.value().decode('utf-8')}")

            else:
                total_count=total_count+1
                logger.info(f"total count:{total_count} in log file with message:{msg.value().decode('utf-8')}")

except KeyboardInterrupt as e:
        print(f"user pressed keyboard input and closed the connection")
finally:
        consumer.close()



