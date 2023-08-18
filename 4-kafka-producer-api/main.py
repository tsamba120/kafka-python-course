import os
import logging
import uuid
from typing import List

from dotenv import load_dotenv
from fastapi import FastAPI
from faker import Faker

from kafka import KafkaAdminClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.producer import KafkaProducer
from kafka.errors import TopicAlreadyExistsError

from commands import CreatePeopleCommand
from entities import Person

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

load_dotenv(verbose=True)

app = FastAPI()

@app.on_event('startup')
async def startup_event():
    client = KafkaAdminClient(bootstrap_servers=os.environ.get("BOOTSTRAP_SERVERS"))
    
    topic =  NewTopic(
                name=os.environ.get("TOPICS_PEOPLE_ADV_NAME"),
                num_partitions=int(os.environ.get("TOPICS_PEOPLE_ADV_PARTITIONS")),
                replication_factor=int(os.environ.get("TOPICS_PEOPLE_ADV_REPLICAS"))
    )

    # create topic, but catch if already exists
    try:
        client.create_topics([topic])
    except TopicAlreadyExistsError:
        logger.warning("Topic already exists")
    finally:
        client.close() # close client
    

@app.get('/hello_world')
async def hello_world():
    return {"message": "Hello World!"}

@app.get('/list_topics')
async def list_topics():
    client = KafkaAdminClient(bootstrap_servers=os.environ.get('BOOTSTRAP_SERVERS'))
    topic_list = client.list_topics()
    return {"message": '\n'.join(topic_list)}


# NOTE: Onward is the Section 3 code

# HELPERS # 
def make_producer() -> KafkaProducer:
    """ Helper method to create a Kafka Producer """
    producer =  KafkaProducer(
        bootstrap_servers=os.environ.get("BOOTSTRAP_SERVERS"),
        linger_ms=int(os.environ.get("TOPICS_PEOPLE_ADV_LINGER_MS")),
        retries=int(os.environ.get("TOPICS_PEOPLE_ADV_RETRIES")),
        max_in_flight_requests_per_connection=int(os.environ.get("TOPICS_PEOPLE_ADV_INFLIGHT_REQS")),
        acks=os.environ.get("TOPICS_PEOPLE_ADV_ACK"),
        )

    return producer

class SuccessHandler:
    def __init__(self, person: Person):
        self.person = person
    
    def __call__(self, rec_metadata):
        logger.info(f"""
            Successfully produced
            person {self.person}
            to topic {rec_metadata.topic}
            and partition {rec_metadata.partition}
            at offset {rec_metadata.offset}
        """)
    
class ErrorHandler:
    def __init__(self, person: Person):
        self.person = person

    def __call__(self, exception):
        logger.error(f"Failed to produce person {self.person}", exc_info=exception)

@app.post('/api/people', status_code=201, response_model=List[Person])
async def create_people(cmd: CreatePeopleCommand) -> List[Person]:
    people: List[Person] = []

    faker = Faker()
    producer: KafkaProducer = make_producer()

    for _ in range(cmd.count):
        person = Person(
            id=str(uuid.uuid4()),
            name=faker.name(),
            title=faker.job().title(),
        )
        people.append(person)
        producer.send(
            topic=os.environ.get('TOPICS_PEOPLE_ADV_NAME'),
            key=person.title.lower().replace(r's+', '-').encode('utf-8'), # Client applications serializes data prior to going into Kafka broker
            value=person.json().encode('utf-8') # uses json() method exposed by Pydantic BaseModel
                ) \
                .add_callback(SuccessHandler(person)) \
                .add_errback(ErrorHandler(person))
    
    producer.flush()

    return people # return with 201 response

