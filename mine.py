
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
import threading
import time
from hashlib import sha256
from kafka import KafkaProducer
from json import dumps
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
from pymongo import MongoClient
from pyspark.streaming import StreamingContext
from pyspark import SparkContext, RDD
from parameters import _PORT, _HOST, _TOPIC, _PARTITION_0, _PARTITION_1

_MAX_NONCE = 2**32
_HASH_TARGET = '000000'
_PROCESSORS = 10


# Global variables used by the solver
last_block_id = -1
last_block_digest = '0'


def calculate_sha(solved: threading.Event, sequence_number: int, transactions: list(), previous_digest: str, nonce_range=range(_MAX_NONCE)) -> (str, int):
    """
    Tests the nonces in the provided range for one that calculates the desired Hash. It is run by multiple threads
    and ends when the nonces are exhausted or aa Hash is found or another thread finds the Hash
    :param solved: Event shared by all the threads to signal that a Hash has been found
    :param sequence_number: The sequence number to include in Hash
    :param transactions: The transactions to include in Hash
    :param previous_digest: The digest of the previous block, to include in Hash
    :param nonce_range: The range of nonces to try
    :return: (digest, nonce) tuple when a Hash is found
    """
    print('Calculating sha:', nonce_range)
    sha = sha256(sequence_number.to_bytes(4))
    for transaction in transactions:
        sha.update(transaction.encode())
    previous_digest = previous_digest.encode()
    for nonce in nonce_range:
        if nonce % 1000 == 0 and solved.is_set():
            print('Aborting sha:', nonce_range)
            return (None, None)
        n_sha = sha.copy()
        n_sha.update(nonce.to_bytes(4))
        n_sha.update(previous_digest)
        digest = n_sha.hexdigest()
        if digest.startswith(_HASH_TARGET):
            solved.set()
            return (digest, nonce)


def create_block(sequence_number: int, transactions: [], nonce: int, digest: str, mining_time: int) -> {}:
    """
    Once a nonce is found the block is constructed as a quintuple that contains
    :param sequence_number: The block serial number.
    :param transactions: The list of transactions that are contained in the block
    :param nonce: The value of the nonce that resulted to the successful mining of the block
    :param digest: The block’s digest
    :param mining_time: The time it took to mine the block
    :return: The block as a dictionary
    """
    block = {
        'sequence_number': sequence_number,
        'transactions': transactions,
        'nonce': nonce,
        'digest': digest,
        'mining_time': mining_time
    }
    return block

# Connection to Kafka
kafka_producer = None
blockchain = list()


def generate_block(invocation_time, rdd:RDD, transactions=None) -> None:
    """
    Generates a block from the contents of the rdd using threads
    For the purposes of this project PoW mining will be used.
    Then the problem of mining is to find an integer value n (called nonce), such that the digest of the quintuple
    1. the sequence number of the block,
    2. the transactions the block contains,
    3. the value of the nonce,
    4. the value of the digest of the previous block
    has a certain number of leading zeros.
    The only thing that can vary in the previous list (contents of the block) is the value of the nonce.
    The number of leading zeros determines the difficulty level of block mining. The more the number
    of leading zeros required the more difficult the mining problem becomes. Fr the purposes of the
    project we assume the level of difficulty to be 3, i.e., 3 leading zeros of the digest.
    :param invocation_time: Not used
    :param rdd: The RDD containing the transactions
    :param transactions: The transactions to use if no RDD is provided
    :return: None
    """
    global last_block_id
    global last_block_digest
    print('Genarating block: ', last_block_id+1)
    current_nonce = -1
    start_time = time.time()
    if rdd is not None:
        transactions = rdd.collect()
    # create the manager to coordinate shared objects like the event
    with Manager() as manager:
        # create an event to shut down all running tasks
        solved = manager.Event()
        # Get all the transactions together
        step = _MAX_NONCE//_PROCESSORS
        params_transactions = list()
        params_sequence_number = list()
        params_previous_digest = list()
        params_nonce_range = list()
        params_solved = list()
        for i in range(0, _PROCESSORS):
            params_solved.append(solved)
            params_sequence_number.append(last_block_id+1)
            params_transactions.append(transactions)
            params_previous_digest.append(last_block_digest)
            params_nonce_range.append(range(i*step, (i+1)*step))
        with ProcessPoolExecutor(max_workers=_PROCESSORS) as executor:
            futures = executor.map(calculate_sha,
                                   params_solved,
                                   params_sequence_number,
                                   params_transactions,
                                   params_previous_digest,
                                   params_nonce_range)
        for result in futures:
            digest, nonce = result
            if digest is not None:
                last_block_digest = digest
                current_nonce = nonce
        # Get the current_nonce
        print('Found nonce: ', current_nonce)
        # Create the block
        last_block_id = last_block_id+1
        mining_time = time.time() - start_time
        block = create_block(last_block_id, transactions, current_nonce, last_block_digest, mining_time)
        # Write the block to kafka
        partition = _PARTITION_0
        if last_block_id % 2 == 1:
            partition = _PARTITION_1
        kafka_producer.send(_TOPIC, block, partition=partition)
        print(f"[*] Send block to Kafka: {block}")
        blockchain.append(block)
        # Print the whole blockchain
        print("Full blockchain:")
        for block in blockchain:
            print(f"    {block['sequence_number']} - {block['digest']} - {block['mining_time']}")


def run_spark_listener(host=_HOST, port=_PORT):
    global sc
    # Create SparkContext and StreamingContext
    sc = SparkContext("local[12]", "BlockchainMine")
    sc.setLogLevel("ERROR")
    # Read messages every second
    ssc = StreamingContext(sc, 1)

    # Create a socket DStream to listen for transactions
    transactions_stream = ssc.socketTextStream(host, port)

    # Process transactions and mine blocks
    # Take all messages in the last 10 #120 seconds and pass them
    transactions_stream.window(10,10).foreachRDD(generate_block)

    # Start the StreamingContext
    ssc.start()
    ssc.awaitTermination()


def connect_kafka() -> KafkaProducer:
    """Connects to Kafka and returns a Producer"""
    szer = lambda x: dumps(x).encode('utf-8')
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=szer)
    # bin/kafka-topics.sh --create --topic Blocks --partitions 2 --bootstrap-server localhost:9092
    return producer


def main():
    global kafka_producer
    global last_block_id
    global last_block_digest
    kafka_producer = connect_kafka()
    # Check if you need to create the genesis block:
    # MongoDB configuration
    client = MongoClient('localhost', 27017)
    db = client['itc6107']
    blocks_collection = db['blocks']
    # Get the block with the highest sequence number
    block = blocks_collection.find_one(sort=[("sequence_number", -1)])
    """
    The very first block of the chain, the genesis block, is hand crafted. 
    You may consider that the only transaction it contains is the string ‘Genesis block’, 
    its sequence number is 0 and the hash of the previous block is the string ‘0’ 
    as no previous block exists. Once the genesis block is constructed and its digest computed, 
    additional blocks can be constructed and added to the blockchain containing transactions 
    from a stream of transactions.
    """
    if block is None:
        last_block_id = -1
        generate_block(rdd=None, transactions=['Genesis block'])
    else:
        last_block_id = block['sequence_number']
        last_block_digest = block['digest']
    run_spark_listener()

if __name__ == "__main__":
    print(__doc__)
    main()
