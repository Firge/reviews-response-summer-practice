import os
import time
from kafka import KafkaConsumer
import psycopg2
import json
import sys
from time import sleep

# Конфигурация из environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC_RESPONSES = os.getenv('TOPIC_RESPONSES')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')


def create_kafka_consumer():
    """Создает Kafka consumer с retry логикой"""
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                TOPIC_RESPONSES,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                consumer_timeout_ms=1000
            )
            print("Kafka consumer created successfully")
            return consumer
        except Exception as e:
            print(f"Attempt {attempt + 1} failed to create Kafka consumer: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Failed to create Kafka consumer after all retries")
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


def save_response(data):
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # Вставляем ответ
        cur.execute(
            "INSERT INTO responses (review_id, content) VALUES (%s, %s)",
            (data['review_id'], data['response'])
        )

        conn.commit()
        cur.close()
        print(f"Saved response for review {data['review_id']}: {data['response']}")
    finally:
        conn.close()


if __name__ == "__main__":
    print("Publisher service started...")
    try:
        consumer = create_kafka_consumer()

        sleep(30)
        print("Waiting for responses...")
        for message in consumer:
            save_response(message.value)

        print("Publisher service completed")
    except Exception as e:
        print(f"Publisher service failed: {e}")
        sys.exit(1)