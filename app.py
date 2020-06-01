from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import json
import time
import os
import random
import csv
from itertools import cycle
from time import sleep
import logging

BROKER_URL = os.environ['BROKER_URL']
KAFKA_TOPIC = os.environ['KAFKA_TOPIC']
MAX_RETRIES = 100

logging.basicConfig(level=logging.INFO)
logging.info(f"Using broker at {BROKER_URL}")
logging.info(f"Using topic '{KAFKA_TOPIC}'")

def get_producer():
    retries = 0
    while retries < MAX_RETRIES:
        try:
            result = KafkaProducer(bootstrap_servers=[BROKER_URL])
            return result
        except KafkaError:
            logging.warning("No broker available. Retrying.")
            retries += 1
            sleep(2)

producer = get_producer()

def read_data(filename):
    with open(filename, encoding='utf-8') as f:
        reader = csv.reader(f)
        csv_string = list(reader)
    class_one = []
    class_zero=[]
    for each in csv_string[1:]:
        if each[-1] == "0":
            class_zero.append(each)
        else:
            class_one.append(each)

    return [class_zero,class_one]

def sendMessage(payload,topic,producer):
    producer.send(topic, payload)
    logging.info(f"Sending {payload}")
    producer.flush()


messages = read_data('data/data.csv')

class_zero = messages[0]
class_one = messages[1]

one_pointer=0
zero_pointer=0
print(len(class_zero))
print(len(class_one))


while True:
    prob = random.randint(1,6)
    if prob == 5:
        if one_pointer < len(class_one):
            sendMessage(json.dumps(class_one[one_pointer]).encode('utf-8'), KAFKA_TOPIC, producer)
            one_pointer = one_pointer + 1
        else:
            one_pointer = 0
            sendMessage(json.dumps(class_one[one_pointer]).encode('utf-8'), KAFKA_TOPIC, producer)
    else:
        if zero_pointer < len(class_zero):
            sendMessage(json.dumps(class_zero[zero_pointer]).encode('utf-8'), KAFKA_TOPIC, producer)
            zero_pointer = zero_pointer + 1
        else:
            one_pointer = 0
            sendMessage(json.dumps(class_zero[zero_pointer]).encode('utf-8'), KAFKA_TOPIC, producer)

    time.sleep(random.randint(2,6))