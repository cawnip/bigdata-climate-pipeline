import os
import json
import time
import logging
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime, timezone

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


MESSAGE_RATE = int(os.getenv('MESSAGE_RATE', '10'))


def load_data(filepath):
    df = pd.read_csv(filepath)
    df['date'] = df['date'].astype(str)
    df = df.where(pd.notnull(df), None)
    return df


def row_to_message(row):
    return {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'station_id': row.get('station_id'),
        'city_name': row.get('city_name'),
        'date': row.get('date'),
        'season': row.get('season'),
        'avg_temp_c': row.get('avg_temp_c'),
        'min_temp_c': row.get('min_temp_c'),
        'max_temp_c': row.get('max_temp_c'),
        'precipitation_mm': row.get('precipitation_mm'),
        'snow_depth_mm': row.get('snow_depth_mm'),
        'avg_wind_dir_deg': row.get('avg_wind_dir_deg'),
        'avg_wind_speed_kmh': row.get('avg_wind_speed_kmh'),
        'peak_wind_gust_kmh': row.get('peak_wind_gust_kmh'),
        'avg_sea_level_pres_hpa': row.get('avg_sea_level_pres_hpa'),
        'sunshine_total_min': row.get('sunshine_total_min'),
    }


def stream_data(producer, df):
    total = 0
    delay = 1.0 / MESSAGE_RATE

    while True:
        for _, row in df.iterrows():
            msg = row_to_message(row.to_dict())
            producer.send(TOPIC, value=msg)
            total += 1
            if total % 100 == 0:
                logger.info(f"Gonderilen mesaj sayisi: {total} | Topic: {TOPIC}")
            time.sleep(delay)
        logger.info(f"CSV bitti, basa donuluyor. Toplam: {total} mesaj")


if __name__ == '__main__':
    logger.info(f"Producer basliyor | Sunucu: {BOOTSTRAP_SERVERS} | Topic: {TOPIC} | Hiz: {MESSAGE_RATE} msg/s")
    try:
        df = load_data(DATA_FILE)
        logger.info(f"Veri yuklendi: {len(df)} satir, dosya: {DATA_FILE}")
        producer = create_producer()
        logger.info("Kafka baglantisi kuruldu, mesajlar gonderiliyor...")
        stream_data(producer, df)
    except KafkaError as e:
        logger.error(f"Kafka hatasi: {e}")
        raise
    except FileNotFoundError:
        logger.error(f"Veri dosyasi bulunamadi: {DATA_FILE}")
        raise
    except Exception as e:
        logger.error(f"Beklenmeyen hata: {e}")
        raise
