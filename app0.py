"""
Write a Python application in file app0.py that reads partition 0 of
topic “Blocks” and adds its contents to a MongoDB collection “blocks”.
"""
from kafka import KafkaConsumer, TopicPartition
from json import loads
from pymongo import MongoClient

print(__doc__)

_TOPIC = 'Blocks'
_PARTITION_0 = 0

# Create the KafkaConsumer
# szer = lambda x: loads(x.decode('utf-8'))
# consumer = KafkaConsumer(#_TOPIC,
#                          bootstrap_servers=['localhost:9092'],
#                          auto_offset_reset='earliest',
#                          enable_auto_commit=True,
#                          # group_id='group1',
#                          value_deserializer=szer)
# consumer.assign([TopicPartition(_TOPIC, _PARTITION_0)])

consumer = ['{message: "Block 1"}']

# Create the connection to MongoDB
# Provide the mongodb atlas url to connect python to mongodb using pymongo
CONNECTION_STRING = "mongodb://localhost:27017"

# Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
mongo_client = MongoClient(CONNECTION_STRING)
itc6107_db = mongo_client['itc6107']
# Access the database and get a reference to collection "blocks"
blocks_collection = itc6107_db['blocks']

for block in consumer:
    print('Received Block from Kafka: ', block)
    blocks_collection.insert_one({"message":block})
