import os
import json
import time
import logging
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'climate-data')
DATA_FILE = os.getenv('DATA_FILE', '../data/sample/climate_sample.csv')


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5
    )


def load_data(filepath):
    df = pd.read_csv(filepath)
    df['date'] = df['date'].astype(str)
    df = df.where(pd.notnull(df), None)
    return df
