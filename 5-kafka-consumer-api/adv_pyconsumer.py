# run the producer in 3-kafka-admin-api/main_1.py
import json
import logging
import os

from dotenv import load_dotenv, find_dotenv
from kafka import TopicPartition, OffsetAndMetadata
from kafka.consumer import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


load_dotenv(verbose=True)

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP")
TOPIC_NAME = os.environ.get("TOPICS_PEOPLE_BASIC_NAME")

CONSUMER_GROUP_ADV = os.environ.get("CONSUMER_GROUP_ADV")
TOPIC_NAME_ADV = os.environ.get("TOPICS_PEOPLE_ADV_NAME")

def people_key_deserializer(key) -> str:
    """
    Desers bytes to String
    """
    return key.decode('utf-8')

def people_value_deserializer(value) -> dict:
    """
    Desesr bytes to dictionary
    """
    return json.loads(value.decode('utf-8'))

def main():
    logger.info(f"""
        Started Python Consumer
        for topic {TOPIC_NAME} 
    """)

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ADV,
        key_deserializer=people_key_deserializer,
        value_deserializer=people_value_deserializer,
        enable_auto_commit=False
    )

    consumer.subscribe([TOPIC_NAME_ADV])

    for record in consumer:
        logger.info(f"""
            Consumed Person {record.value}
            with key '{record.key}'
            from partition {record.partition}
            at offset {record.offset}
        """)

        # Note how we're pulling the necessary record metadata using the record api
        # We do the code below because we disable autocommit (line 46)
        topic_partition = TopicPartition(record.topic, record.partition)
        offset = OffsetAndMetadata(record.offset + 1, record.timestamp) # always commit the offset of the NEXT message to be consumed

        consumer.commit({topic_partition: offset})



if __name__ == "__main__":
    main()
