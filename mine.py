
"""
Write a Spark Streaming application in file mine.py to accept transactions
coming from the server, collect them in 2-minutes intervals, and mine a block
that includes them. The level of parallelism for mining should depend on the
number of cores the processor of your machine. When mining blocks, observe
the performance of the cores of your machine and take a copy of the performance
monitor as the mining process progresses.
When a new block is mined your application must print the whole blockchain
from the genesis block to the newly mined one.

Each time a block is mined all information about the block is written to a Kafka
topic “Blocks” as a JSON object, which must contain the following:
1. The block serial number.
2. The number of transactions that are contained in the block.
3. The block nonce.
4. The block’s digest.
5. The time it took to mine the block.
Topic “Blocks” contains two partitions: 0 and 1. Mined blocks with even serial
numbers are written to partition 0 and blocks with odd serial numbers are written
to partition 1.
"""
import socket
import string
import json
from kafka import KafkaProducer
from json import dumps

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
            kafka_producer.send(_TOPIC, block, partition=_PARTITION_0)
            print(f"[*] Send block to Kafka: {block}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the socket connection
        client_socket.close()

if __name__ == "__main__":
    main()
