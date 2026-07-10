import os
import logging
from confluent_kafka import Consumer, KafkaError
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "openlineage-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "marquez-kafka-consumer")
MARQUEZ_API_URL = os.getenv("MARQUEZ_API_URL", "http://marquez-api:5000/api/v1/lineage")

def main():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    
    logger.info(f"Starting consumer, listening to topic: {KAFKA_TOPIC} at {KAFKA_BOOTSTRAP_SERVERS}")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
            
            # Decode message
            try:
                lineage_event_str = msg.value().decode('utf-8')
                logger.debug(f"Received event: {lineage_event_str[:200]}...")
                
                # Send to Marquez API
                headers = {'Content-Type': 'application/json'}
                response = requests.post(MARQUEZ_API_URL, data=lineage_event_str, headers=headers)
                
                if response.status_code in (200, 201):
                    logger.info("Successfully pushed event to Marquez.")
                else:
                    logger.error(f"Failed to push to Marquez. Status: {response.status_code}, Response: {response.text}")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        logger.info("Consumer stopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
