import pandas as pd
import json
import csv
from pykafka import KafkaClient
from pykafka.exceptions import SocketDisconnectedError, NoBrokersAvailableError
from pyhive import hive
import commands
import sys
import os
import paramiko
import time
import pyhs2
from subprocess import PIPE, Popen
import math

# Python Kafka Client Connection is made.

client = KafkaClient(hosts="hostname:port")
topic = client.topics['topic_res']
#consumer = topic.get_simple_consumer(consumer_timeout_ms=5000,auto_commit_enable=True,reset_offset_on_start=False)

# Connection Error Handle
try:
    consumer = topic.get_simple_consumer(consumer_timeout_ms=5000,auto_commit_enable=True,reset_offset_on_start=False)
    consumer.consume()
    #consumer.commit_offsets()

except (SocketDisconnectedError, NoBrokersAvailableError) as e:
    consumer = topic.get_simple_consumer(consumer_timeout_ms=5000,auto_commit_enable=True,reset_offset_on_start=False,auto_offset_reset=OffsetType.LATEST)
    # use either the above method or the following:
    consumer.stop()
    consumer.start()

# We need to obtain a pandas dataframe, for that we write the read
# string of json into a csv file.

queue_data = open('/data/212708294/try.csv', 'w')
csvwriter = csv.writer(queue_data)
count = 0

for message in consumer:
        if message is not None:
            #print message.value
            emp_data = message.value
            data_parsed = json.loads(emp_data)
            if count == 0:
                header = data_parsed.keys()
                csvwriter.writerow(header)
                count += 1
            csvwriter.writerow(data_parsed.values())

        elif message is None:
            consumer.stop()
            print("error!")
            break
queue_data.close()


sys.exit()
