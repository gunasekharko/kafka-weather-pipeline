from config import BROKER_ADDRESS,TOPIC_RAW,TOPIC_PROCESSED
from quixstreams import Application


#1.connecting to broker
application=Application(
    broker_address=BROKER_ADDRESS,
    auto_offset_reset='earliest',
    consumer_group='weather-window-aggregator-v1'
)
#2.created input and output topic

input_topic=application.topic(TOPIC_RAW,value_deserializer='json')
output_topic=application.topic(TOPIC_PROCESSED,value_serializer='json')

#3. create dataframe
sdf=application.dataframe(topic=input_topic)

def validation_logic(record:dict):
    required=['temperature','humidity','station_id']
    for key in required:
        if key not in record:
            return False
    temp=record['temperature']    
    humidity=record['humidity']

    if not isinstance(temp,(int,float)):
        return False
    if not (-50 <= temp <= 50):
        return False
    if not isinstance(humidity,(int,float)):
        return False
    return True

def tag_validation(record):
    record['isvalid']=validation_logic(record)
    return record

def initial_state(record:dict):
    return {
        'count':1,
        'sum_temp':record['temperature']
    }

def reducer(state:dict,record:dict):
    return{
        'count':state['count']+1,
        'sum_temp':state['sum_temp']+record['temperature']
    }

def formatted_window_result(value,key,timestamp,headers):
    return{
        'start':value['start'],
        'end'  :value['end'],
        'city' :key,
        'count':value['value']['count'],
        'avg_temp':round(value['value']['sum_temp']/value['value']['count'])
    }



sdf=sdf.apply(tag_validation)

sdf=sdf.filter(lambda v : v['isvalid'])

sdf = sdf.group_by(lambda record: record['city'],name='by_city')

sdf=sdf.tumbling_window(duration_ms=50000).reduce(reducer=reducer,initializer=initial_state).final()

sdf=sdf.apply(formatted_window_result,metadata=True)

sdf=sdf.update(lambda r:print(f"window result:{r}"))

sdf.to_topic(output_topic)



if __name__=='__main__':
    application.run(sdf)