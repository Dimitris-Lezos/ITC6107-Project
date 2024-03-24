
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
import threading
import time

from kafka import KafkaProducer
from json import dumps

from pyspark.streaming import StreamingContext

from parameters import _PORT, _HOST, _TOPIC, _PARTITION_0, _PARTITION_1
from pyspark import SparkContext, RDD

print(__doc__)

_MAX_INT = 2**32
_HASH_TARGET = '0000'
_PROCESSORS = 10


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


def solve_block(messages: []) -> None:
    print('Sleeping for 20 seconds, data:', len(messages))
    time.sleep(20)
    print('Finished: ', len(messages))


def collect(rdd:RDD) -> None:
    # Create a thread that will solve the block
    solver = threading.Thread(target=solve_block, args=[rdd.collect()])
    solver.start()


def run_spark_listener(host=_HOST, port=_PORT):
    # Create SparkContext and StreamingContext
    sc = SparkContext("local[5]", "BlockchainMine")
    # Read messages every 5 #10 seconds
    ssc = StreamingContext(sc, 1) #10)

    # Create a socket DStream to listen for transactions
    transactions_stream = ssc.socketTextStream(host, port)

    # Process transactions and mine blocks
    # Take all messages in the last 10 #120 seconds
    transactions_stream.window(10,10).foreachRDD(collect)

    # Start the StreamingContext
    ssc.start()
    ssc.awaitTermination()


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


"""
Once a nonce is found the block is constructed as a quintuple that contains
1. The block serial number.
2. The list of transactions that are contained in the block.
3. The value of the nonce that resulted to the successful mining of the block.
4. The block’s digest.
5. The time it took to mine the block.
"""

"""
The very first block of the chain, the genesis block, is hand crafted. 
You may consider that the only transaction it contains is the string ‘Genesis block’, 
its sequence number is 0 and the hash of the previous block is the string ‘0’ 
as no previous block exists.
"""

"""
Each time a block is mined all information about the block is written to a Kafka topic “Blocks” 
as a JSON object, which must contain the following:
1. The block serial number.
2. The number of transactions that are contained in the block.
3. The block nonce.
4. The block’s digest.
5. The time it took to mine the block.
"""

"""
Topic “Blocks” contains two partitions: 0 and 1. Mined blocks with even serial numbers 
are written to partition 0 and blocks with odd serial numbers are written to partition 1.
"""

def main():
    run_spark_listener()
    # # Create a thread that listens for connections
    # listener = threading.Thread(target=run_spark_listener)
    # listener.start()
    # listener.join()

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
