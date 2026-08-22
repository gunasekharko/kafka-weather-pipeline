from quixstreams import Application
from config import BROKER_ADDRESS,TOPIC_RAW,TOPIC_CLEAN,TOPIC_FLAGGED


app=Application(
    broker_address=BROKER_ADDRESS,
    consumer_group='tx-stateless-cleaner-v3',
    auto_offset_reset='earliest'
)

input_topic=app.topic(name=TOPIC_RAW,value_deserializer='json')
clean_topic=app.topic(name=TOPIC_CLEAN,value_serializer='json')
flagged_topic=app.topic(name=TOPIC_FLAGGED,value_serializer='json')

sdf=app.dataframe(topic=input_topic)


def validate(record:dict):
    # account_id=record['account_id']
    account_id=record.get('account_id',None)
    # amount_cents=record['amount_cents']
    amount_cents=record.get('amount_cents',None)
    if(not account_id or account_id==''):
        return False
    elif(not amount_cents or amount_cents<=0):
        return False
    else:
        return True


# {"tx_id": "TX-102", "account_id": "A2", "amount_cents": -50, "currency": "USD"}

def log_transaction(record: dict):
    tx_id = record.get('tx_id', 'UNKNOWN_TX')
    account_id = record.get('account_id')
    amount_cents = record.get('amount_cents')

    
    print(f"[validated] tx_id={tx_id} account_id={account_id} amount_cents={amount_cents}")


def enrich(record:dict):
    amount_usd=record['amount_cents']/100.0
    record['amount_usd']=amount_usd
    record['is_high_value']=amount_usd>= 50.0
    return record

sdf=sdf.filter(validate)

sdf=sdf.update(lambda v:log_transaction(v))

sdf = sdf.apply(enrich)

sdf = sdf.to_topic(clean_topic)


sdf = sdf.filter(lambda v: v['is_high_value'] == True)
sdf = sdf.update(lambda v: print(f"ALERT: HIGH VALUE TX: {v}"))
sdf = sdf.to_topic(topic=flagged_topic)

if __name__=="__main__":
    app.run(sdf)  


