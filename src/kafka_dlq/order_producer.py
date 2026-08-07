from confluent_kafka import Producer
import json
import time
import uuid
import random
import logging

logging.basicConfig(
    format="%(levelname)s %(name)s %(asctime)s :%(message)s",
    datefmt ="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

conf={
    'bootstrap.servers':'localhost:9092',
    'acks':"all",
    'enable.idempotence':True
}

producer=Producer(conf)

customers=['c001','c002','c003','c004','c005']

# value=make_valid_order(orders['order_id'],orders['customer_id'],orders['amount'])
def make_valid_order(orderid,customer_id,amount):
    customer_id=customer_id
    amount=amount
    return {"order_id":orderid,"amount":amount,"customer_id":customer_id}

def make_poison_pill(orderid,customer_id,amount):
    return b'{"order_id": BROKEN_JSON!!!'


topic='orders'

def delivery(err,msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(
            f"Delivered message for Key='{msg.key().decode('utf-8')}' "
            f"to Partition [{msg.partition()}] at Offset {msg.offset()}"
        )



try:
    while True:
      key_customer=random.choice(customers)
      is_poison=random.random() < 0.20
      orders={
        "order_id":str(uuid.uuid4()),
        "customer_id":key_customer,
        "amount" : random.uniform(-10,2000) if random.random()<0.8 else None
      } 
      if is_poison:
        raw=make_poison_pill(orderid=orders['order_id'],customer_id=orders['order_id'],amount=orders['amount'])
      else:
        raw = json.dumps(make_valid_order(orderid=orders['order_id'], customer_id=orders['customer_id'], amount=orders['amount'])).encode('utf-8')
      producer.produce(topic=topic,value=raw,key=key_customer,on_delivery=delivery)
      producer.poll(0.1)
      time.sleep(1)
except KeyboardInterrupt as e:
         logging.error(f"user used control + c option to producing the data to consumer")
finally:
 producer.flush()