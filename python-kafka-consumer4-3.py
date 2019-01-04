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

queue_data = open('pathtocsvfile', 'w')
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


# Convertion from csv to pandas df

df = pd.read_csv('/data/212708294/try.csv')

# Hive Connection via the library usage, phys2
with pyhs2.connect(host='ip-10-230-245-94.ec2.internal',port=10000,authMechanism="KERBEROS")as conn:
  with conn.cursor()as cur:
    cur.execute("CREATE TABLE default.lasttry (ems_system_id int, adi_flight_record_number int, MaxOffset double, parameter_id string, value bigint) row format delimited fields terminated by ','")
    for row in df.itertuples(index=False, name='Pandas'):
        #print row
        #try:

        # NotANumber Control for row elements casted to float
        # This part makes size difference between hive table and csv file
        if math.isnan(float(row[4])) or math.isnan(float(row[2])) :
            continue
        mytuple = (int(row[0]),int(row[1]),float(row[2]),row[3],float(row[4]))
        print(mytuple)

        query = "INSERT INTO TABLE default.lasttry VALUES {}".format(mytuple)
        cur.execute(query)

    #data = cur.fetchall()
    #print data
    conn.close()
        #except:

        #query = "INSERT INTO TABLE default.Topic_res55 (ems_system_id, adi_flight_record_number, MaxOffset, parameter_id, value) VALUES ({},{},{},{},{})".format(getattr(row, "ems_system_id"),getattr(row, "adi_flight_record_number"),getattr(row, "MaxOffset"),getattr(row, "parameter_id"),getattr(row, "value"))

sys.exit()


sys.exit()
