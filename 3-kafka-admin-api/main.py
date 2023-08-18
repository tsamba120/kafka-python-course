import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI

from kafka import KafkaAdminClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError

logger = logging.getLogger()
load_dotenv(verbose=True)

app = FastAPI()

@app.on_event('startup')
async def startup_event():
    client = KafkaAdminClient(bootstrap_servers=os.environ.get("BOOTSTRAP_SERVERS"))
    
    topics = [
        NewTopic(
            name=os.environ.get("TOPICS_PEOPLE_BASIC_NAME"),
            num_partitions=int(os.environ.get("TOPICS_PEOPLE_BASIC_PARTITIONS")),
            replication_factor=int(os.environ.get("TOPICS_PEOPLE_BASIC_REPLICAS"))
        ),
        NewTopic(
            name=f'{os.environ.get("TOPICS_PEOPLE_BASIC_NAME")}-short',
            num_partitions=int(os.environ.get("TOPICS_PEOPLE_BASIC_PARTITIONS")),
            replication_factor=int(os.environ.get("TOPICS_PEOPLE_BASIC_REPLICAS")),
            topic_configs={
                'retention.ms': '360000'
            }
        ),
    ]
    # create topic, but catch if already exists
     # loop through list of topics and send info + configs to client to create
    for topic in topics:
        try:
            client.create_topics([topic])
        except TopicAlreadyExistsError:
            logger.warning("Topic already exists")

    # NOTE: Update the first topic's configs upon start up
    cfg_resource_update = ConfigResource(
        ConfigResourceType.TOPIC,
        os.environ.get("TOPICS_PEOPLE_BASIC_NAME"),
        configs={'retention.ms': '360000'}
    )
    client.alter_configs([cfg_resource_update])
    
    # close client
    client.close()

@app.get('/hello_world')
async def hello_world():
    return {"message": "Hello World!"}

@app.get('/list_topics')
async def list_topics():
  client = KafkaAdminClient(bootstrap_servers=os.environ.get('BOOTSTRAP_SERVERS'))
  topic_list = client.list_topics()
  return {"message": '\n'.join(topic_list)}
