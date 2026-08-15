from quixstreams import Application
from config import BROKER_ADDRESS , TOPIC_PROCESSED , TOPIC_RAW,CITIES

application=Application(
    broker_address=BROKER_ADDRESS,
    consumer_group='weather_processor',
    auto_offset_reset='earliest'
)


input_topic=application.topic(TOPIC_RAW,value_deserializer='json')
output_topic=application.topic(TOPIC_PROCESSED,value_deserializer='json',value_serializer='json')

sdf=application.dataframe(topic=input_topic)

sdf['temperature']=(sdf['temperature']*(9/5))+32

sdf = sdf.update(lambda val: print(f"Transformed: {val}"))

sdf.to_topic(output_topic)

application.run(sdf)