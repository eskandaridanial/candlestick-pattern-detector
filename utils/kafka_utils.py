import sys

sys.path.append('')

from config import *
from kafka import KafkaProducer
from kafka import KafkaConsumer

# -------------------------------------------------------------------------------------

def consume():
    return KafkaConsumer(KAFKA_TOPIC)

# -------------------------------------------------------------------------------------

def produce(message):
    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST + ':' + KAFKA_PORT)
    producer.send(topic=KAFKA_TOPIC , value=bytes(message, KAFKA_MSG_ENCODING))

# -------------------------------------------------------------------------------------