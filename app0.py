"""
Write a Python application in file app0.py that reads partition 0 of
topic “Blocks” and adds its contents to a MongoDB collection “blocks”.
"""
from parameters import _PARTITION_0
import app

print(__doc__)

app.kafka_to_mongo(_PARTITION_0)