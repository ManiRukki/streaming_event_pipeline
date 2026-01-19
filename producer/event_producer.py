import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

# Configuration
KAFKA_TOPIC = "user_clicks"
KAFKA_BROKER = "localhost:9092"

def generate_click_event():
    """Simulates a user click event"""
    users = [f"user_{i}" for i in range(1, 101)]
    pages = ["/home", "/cart", "/checkout", "/product/abc", "/product/xyz"]
    
    return {
        "event_time": datetime.now().isoformat(),
        "user_id": random.choice(users),
        "url": random.choice(pages),
        "region": random.choice(["US", "EU", "APAC"]),
        "browser": random.choice(["Chrome", "Firefox", "Safari"])
    }

def run_producer():
    print(f"🚀 Starting Kafka Producer for topic: {KAFKA_TOPIC}")
    # Note: In a real run, this needs a running Kafka instance.
    # We allow connection errors to fail gracefully if Kafka isn't up.
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"⚠️ Could not connect to Kafka: {e}")
        print("💡 Ensure Docker containers are running.")
        return

    while True:
        event = generate_click_event()
        producer.send(KAFKA_TOPIC, event)
        print(f"Sales Event sent: {event['user_id']} -> {event['url']}")
        time.sleep(0.5) # Simulate 2 events per second

if __name__ == "__main__":
    run_producer()
