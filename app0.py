"""
Write a Python application in file app0.py that reads partition 0 of
topic “Blocks” and adds its contents to a MongoDB collection “blocks”.
"""
from kafka import KafkaConsumer, TopicPartition
from json import loads

print(__doc__)

_TOPIC = 'Blocks'
_PARTITION_0 = 0

szer = lambda x: loads(x.decode('utf-8'))
consumer = KafkaConsumer(#_TOPIC,
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         # group_id='group1',
                         value_deserializer=szer)
consumer.assign([TopicPartition(_TOPIC, _PARTITION_0)])

for block in consumer:
    print('Received Block from Kafka: ', block)
