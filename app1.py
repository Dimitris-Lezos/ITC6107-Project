"""
Similarly for a second application app1.py that reads partition 1 of “Blocks”
and writes its contents to the same MongoDB collection.
"""
from kafka import KafkaConsumer, TopicPartition
from json import loads

print(__doc__)

_TOPIC = 'Blocks'
_PARTITION_1 = 1

szer = lambda x: loads(x.decode('utf-8'))
consumer = KafkaConsumer(#_TOPIC,
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         # group_id='group1',
                         value_deserializer=szer)
consumer.assign([TopicPartition(_TOPIC, _PARTITION_1)])

for block in consumer:
    print('Received Block from Kafka: ', block)
