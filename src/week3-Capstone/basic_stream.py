from quixstreams import Application
from config import BROKER_ADDRESS,TOPIC_RAW,TOPIC_PROCESSED

app=Application(
    broker_address=BROKER_ADDRESS,
    consumer_group='transaction-normalizer-v1',
    auto_offset_reset='earliest'
)

input_topic=app.topic(name=TOPIC_RAW,value_deserializer='json')
output_topic=app.topic(name=TOPIC_PROCESSED,value_serializer='json')


#1.create a datasteam with input topic

sdf=app.dataframe(topic=input_topic)

# {"tx_id": "TX-101", "account_id": "A1", "amount_cents": 5400, "currency": "USD"}
# {"tx_id": "TX-102", "account_id": "A2", "amount_cents": -50, "currency": "USD"}
# {"tx_id": "TX-103", "account_id": "A3", "amount_cents": 1299, "currency": "USD"}

def add_usd_amount(v):
    v['amount_usd']=v['amount_cents']/100.0
    return v

sdf = sdf.filter(lambda v: v.get('amount_cents') is not None and v['amount_cents'] > 0)

sdf=sdf.apply(add_usd_amount)

sdf=sdf.update(lambda val:print(f"processed:{val}"))

sdf=sdf.to_topic(output_topic)

if __name__=="__main__":
    app.run(sdf)