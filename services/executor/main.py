import time
from kafka import KafkaConsumer, KafkaProducer
import json
import sys
from model import generate_response
from time import sleep
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC_REVIEWS = os.getenv('TOPIC_REVIEWS')
TOPIC_RESPONSES = os.getenv('TOPIC_RESPONSES')


def create_kafka_consumer():
    """Создает Kafka consumer с retry логикой"""
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                TOPIC_REVIEWS,
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
                return None
            else:
                print("Failed to create Kafka consumer after all retries")
                raise
    return None


def create_kafka_producer():
    """Создает Kafka producer для отправки ответов"""
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=30000
            )
            print("Kafka response producer created successfully")
            return producer
        except Exception as e:
            print(f"Attempt {attempt + 1} failed to create Kafka producer: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Failed to create Kafka producer after all retries")
                raise


def process_review(review_data, producer):
    review_id = review_data['review_id']
    review = review_data['review']
    response = generate_response(review)
    result = {
        'review_id': review_id,
        'review': review,
        'response': response
    }
    producer.send(TOPIC_RESPONSES, value=result)
    print(f"Processed review {review_id}: {review} -> {response}")


if __name__ == "__main__":
    print("Executor service started...")
    try:
        consumer = create_kafka_consumer()
        producer = create_kafka_producer()

        print("Waiting for reviews...")
        sleep(20)
        for message in consumer:
            process_review(message.value, producer)

        producer.flush()
        print("Executor service completed")
    except Exception as e:
        print(f"Executor service failed: {e}")
        sys.exit(1)