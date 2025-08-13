import os
import time
import psycopg2
from kafka import KafkaProducer
import json
import sys

# Конфигурация из environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC_REVIEWS = os.getenv('TOPIC_REVIEWS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')


def create_kafka_producer():
    """Создает Kafka producer с retry логикой"""
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=30000
            )
            print("Kafka producer created successfully")
            return producer
        except Exception as e:
            print(f"Attempt {attempt + 1} failed to create Kafka producer: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Failed to create Kafka producer after all retries")
                raise


def get_db_connection():
    """Создает подключение к PostgreSQL с retry логикой"""
    max_retries = 10
    retry_delay = 3

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )
            print("Database connection established")
            return conn
        except Exception as e:
            print(f"Attempt {attempt + 1} failed to connect to database: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Failed to connect to database after all retries")
                raise


def fetch_reviews():
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, content FROM reviews")
        reviews = cur.fetchall()
        cur.close()
        print(f"Fetched {len(reviews)} reviews from database")
        return reviews
    finally:
        conn.close()


def send_reviews_to_kafka(reviews, producer):
    for review_id, content in reviews:
        message = {
            'review_id': review_id,
            'review': content
        }
        producer.send(TOPIC_REVIEWS, value=message)
        print(f"Sent review {review_id}: {content}")
        time.sleep(1)
    producer.flush()
    print("All reviews sent to Kafka")


if __name__ == "__main__":
    print("Parser service started...")
    try:
        producer = create_kafka_producer()
        reviews = fetch_reviews()
        send_reviews_to_kafka(reviews, producer)
        print("Parser service completed successfully")
    except Exception as e:
        print(f"Parser service failed: {e}")
        sys.exit(1)