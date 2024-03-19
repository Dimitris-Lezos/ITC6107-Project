"""
Write a Python application in file mongoq.py that answers the following:
1. For a given block serial number, what is its nonce value, digest, and number of transactions contained in the block.
2. Which of the blocks mined so far has the smallest mining time.
3. Which is the average and cumulative mining time of all blocks mined so far.
4. Which block(s) has the largest number of transactions in them.
"""
import socket
import string
import json
from kafka import KafkaProducer
from json import dumps
from pymongo import MongoClient
from bson.son import SON


print(__doc__)

_PORT = 9999
_HOST = 'localhost'
_TOPIC = 'Blocks'
_PARTITION_0 = 0
_PARTITION_1 = 1

def connect_server(host=_HOST, port=_PORT) -> socket:
    """Returns a socket that receives messages from host:port"""
    # Create a socket object
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Connect to the server
        client_socket.connect((host, port))
        print(f"[*] Connected to {host}:{port}")
    except Exception as e:
        print(f"Error: {e}")
    return client_socket


def connect_kafka() -> KafkaProducer:
    """Connects to Kafka and returns a Producer"""
    szer = lambda x: dumps(x).encode('utf-8')
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=szer)
    # bin/kafka-topics.sh --create --topic Blocks --partitions 2 --bootstrap-server localhost:9092
    return producer


def create_block(message: string) -> json:
    return json.loads('''
        {
            "message": "''' + message + '''"
        }
    ''')


def main():
    try:
        kafka_producer = connect_kafka()
        # Replace client_socket with a Spark Stream that reads every 120 seconds
        # Can we parallelize the Hashing with Spark?
        client_socket = connect_server()
        while True:
            # Receive data from the server
            message = client_socket.recv(1024).decode('utf-8')
            print(f"[*] Received message from the server: {message}")
            if None == message or len(message) == 0:
                break
            block = create_block(message)
            # THIS is a bug, because the szer serializer expects a Dictionary, not a JSON object!
            kafka_producer.send(_TOPIC, block, partition=_PARTITION_0)
            print(f"[*] Send block to Kafka: {block}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the socket connection
        client_socket.close()

if __name__ == "__main__":
    main()
