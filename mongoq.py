"""
Write a Python application in file mongoq.py that answers the following:
1. For a given block serial number, what is its nonce value, digest, and number of transactions contained in the block.
2. Which of the blocks mined so far has the smallest mining time.
3. Which is the average and cumulative mining time of all blocks mined so far.
4. Which block(s) has the largest number of transactions in them.
"""
from pymongo import MongoClient

# MongoDB configuration
client = MongoClient('localhost', 27017)
db = client['itc6107']
blocks_collection = db['blocks']


def get_block_info(serial_number):
    """
    Returns information of a block based on a serial number
    :param serial_number: The serial number to search for
    :return: Details of the bockl with the provided serial number
    """
    block = blocks_collection.find_one({"sequence_number": serial_number})
    if block:
        return {
            "nonce": block["nonce"],
            "digest": block["digest"],
            "num_transactions": len(block["transactions"])
        }
    else:
        return "Block not found"


def get_block_with_smallest_mining_time():
    """
    Returns the block with the smallest mining time
    :return: the block with the smallest mining time
    """
    block = blocks_collection.find_one(sort=[("mining_time", 1)])
    return block


def get_average_and_cumulative_mining_time():
    """
    Returns the average and cumulative mining time
    :return: the average and cumulative mining time
    """
    pipeline = [
        {"$group": {"_id": None, "avg_time": {"$avg": "$mining_time"}, "total_time": {"$sum": "$mining_time"}}}
    ]
    result = list(blocks_collection.aggregate(pipeline))
    if result:
        del result[0]["_id"]
        return result[0]
    else:
        return {"avg_time": 0, "total_time": 0}


def get_block_with_most_transactions():
    """
    Returns the block with the most transactions
    :return: the block with the most transactions
    """
    pipeline = [
        {"$unwind": "$transactions"},
        {"$group": {"_id": "$_id", "sequence_number": {"$max": "$sequence_number"}, "num_transactions": {"$sum": 1}}},
        {"$sort": {"num_transactions": -1}},
        {"$limit": 1}
    ]
    block = list(blocks_collection.aggregate(pipeline))[0]
    del block["_id"]
    return block


if __name__ == "__main__":
    while True:
        try:
            block_sequence_number = input('\nEnter a block sequence number:')
            # Query 1
            print("Block", block_sequence_number, "info:", get_block_info(int(block_sequence_number)))
            # Query 2
            print("Smallest mining time:", get_block_with_smallest_mining_time())
            # Query 3
            print("Average and Cumulative mining time:", get_average_and_cumulative_mining_time())
            # Query 4
            print("Block with most transactions:", get_block_with_most_transactions())
        except Exception as e:
            print("Error ", e)
        print("-----------------------------------------------------------------------")







