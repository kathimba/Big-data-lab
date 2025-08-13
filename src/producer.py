import json
import time
import random
import logging
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# -------------------- CONFIG --------------------
KAFKA_TOPIC = "transactions"
KAFKA_SERVER = "localhost:9092"
MESSAGE_INTERVAL = 3  # seconds
# -------------------------------------------------

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Sample locations for transactions
LOCATIONS = ["Nairobi", "Lagos", "Kampala", "Cairo", "Accra", "Addis Ababa"]

def generate_transaction():
    return {
        "user_id": random.randint(1, 1000),
        "amount": round(random.uniform(10, 5000), 2),
        "location": random.choice(LOCATIONS),
        "timestamp": time.time()
    }

def create_kafka_topic():
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
        topic = NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
        admin.create_topics([topic])
        logging.info(f"✅ Topic '{KAFKA_TOPIC}' created.")
    except TopicAlreadyExistsError:
        logging.info(f"⚠️ Topic '{KAFKA_TOPIC}' already exists.")
    except NoBrokersAvailable:
        logging.error("❌ Kafka broker not available. Is Kafka running?")
        exit(1)
    finally:
        if 'admin' in locals():
            admin.close()

def start_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logging.info(f"✅ Connected to Kafka at {KAFKA_SERVER}")
        return producer
    except NoBrokersAvailable:
        logging.error("❌ Failed to connect to Kafka broker.")
        exit(1)

def main():
    logging.info("🚀 Starting Kafka Producer...")
    # create_kafka_topic()
    producer = start_producer()

    try:
        while True:
            transaction = generate_transaction()
            producer.send(KAFKA_TOPIC, value=transaction)
            logging.info(f"📤 Sent: {transaction}")
            time.sleep(MESSAGE_INTERVAL)
    except KeyboardInterrupt:
        logging.info("🛑 Producer stopped by user.")
    finally:
        producer.flush()
        producer.close()
        logging.info("✅ Producer shut down cleanly.")

if __name__ == "__main__":
    main()
