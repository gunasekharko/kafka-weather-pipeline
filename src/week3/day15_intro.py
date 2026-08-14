from quixstreams import Application

app=Application(
    broker_address='localhost:9092',
    consumer_group='weather-stream_intro',
    auto_offset_reset='earliest'
)


topic=app.topic('weather_sensor_raw',value_deserializer='json')

sdf=app.dataframe(topic=topic)

sdf.print()
app.run()