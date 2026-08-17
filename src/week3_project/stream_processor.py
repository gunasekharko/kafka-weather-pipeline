from confluent_kafka import Producer
from quixstreams import Application
from config import BROKER_ADDRESS , TOPIC_PROCESSED , TOPIC_RAW,CITIES,TOPIC_DLQ,TOPIC_ALERTS
import json

application=Application(
    broker_address=BROKER_ADDRESS,
    consumer_group='weather_processor_v2',
    auto_offset_reset='earliest'
)

dlq_producer=Producer({'bootstrap.servers': BROKER_ADDRESS})
        

input_topic=application.topic(TOPIC_RAW,value_deserializer='json')
output_topic=application.topic(TOPIC_PROCESSED,value_deserializer='json',value_serializer='json')
dlf_topic=application.topic(TOPIC_DLQ,value_deserializer='json',value_serializer='json')
alert_topic=application.topic(TOPIC_ALERTS,value_deserializer='json',value_serializer='json')

def route_to_dlq_if_invalid(record: dict):
    if not record.get('isvalid', True):
        
        key = record.get('city', 'invalid').encode('utf-8')
        
        value = json.dumps(record).encode('utf-8')
        
        dlq_producer.produce(
            topic=TOPIC_DLQ, 
            key=key, 
            value=value
        )
        dlq_producer.poll(0)

        

def validate_record(record):
    required_keys = ['temperature', 'humidity', 'station_id']
    for key in required_keys:
        if key not in record:
            return False
    temp=record['temperature']
    humidity=record['humidity']

    if not isinstance(temp,(int,float)):
        return False
    if not (-50 <=temp<=60):
        return False
    if not isinstance(humidity,(int,float)):
        return False
    return True

def tag_validity(record):
    record['isvalid']=validate_record(record)
    return record

def enrich_record(record):
    temp_c=record['temperature']
    humidity=record['humidity']

    record['temperature_f']=(temp_c * 9/5) + 32

    if temp_c < 25:
        record['severity'] = 'LOW'
    elif 25 <= temp_c <= 35:
        record['severity'] = 'MEDIUM'
    else:
        record['severity'] = 'HIGH'
    record['heat_index'] = (temp_c * 1.0) + (humidity * 0.1)
    record.pop('isvalid', None)

    return record



sdf=application.dataframe(topic=input_topic)

sdf=sdf.apply(tag_validity)

sdf = sdf.update(route_to_dlq_if_invalid)

sdf = sdf.filter(lambda r: r['isvalid'])

sdf = sdf.apply(enrich_record)

sdf = sdf.update(lambda val: print(f"valid and enriched: {val}"))

sdf.filter(lambda val: val['severity'] == 'HIGH').to_topic(alert_topic)

sdf.to_topic(output_topic)

if __name__ == '__main__':
    application.run(sdf)