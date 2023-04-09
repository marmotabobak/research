import asyncio
from dataclasses import dataclass
from typing import List, Dict, Callable
import logging
import random

from sqlalchemy import Column, BigInteger, Text, DateTime, create_engine
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import create_async_engine
from confluent_kafka import Consumer, TopicPartition


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


Base = declarative_base()


class Sessions(Base):
    __tablename__ = 'sessions'
    __table_args__ = {'schema': 'modorch'}
    sessions_id = Column('sessions_id', BigInteger, quote=False, primary_key=True)
    session_id = Column('session_id', Text, quote=False)
    session_datetime = Column('session_datetime', DateTime, quote=False)


@dataclass
class PostgresClient:
    user: str
    password: str
    host: str
    port: int
    db_name: str

    engine = None
    async_engine = None

    session = None
    async_session = None

    def create_engine(self):
        url = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}'
        self.engine = create_engine(url)
        self.engine.connect()

    def create_async_engine(self):
        url = f'postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}'
        self.async_engine = create_async_engine(url)
        self.async_engine.connect()

    def create_engines(self) -> None:
        self.create_engine()

    def create_schemas(self, schemas: List[str]) -> None:
        self.session = Session(bind=self.engine)
        for schema in schemas:
            try:
                self.engine.execute(CreateSchema(schema))
                log.info(f'[x] {schema} schema created')
            except ProgrammingError:
                log.warning(f'Error while creating {schema} schema - perhaps it already exists. Skipping...')
            except Exception:
                raise

    def drop_all_tables(self, declarative_base_class):
        declarative_base_class.metadata.drop_all(self.engine)

    def create_all_tables(self, declarative_base_class):
        declarative_base_class.metadata.create_all(self.engine)

    def drop_and_create_all_tables(self):
        self.drop_all_tables()
        self.create_all_tables()

    async def new_data_from_kafka(self, msg_value: str, request_id: int):
        print(f'START PROCESSING {request_id}: {msg_value}')
        try:
            seconds_sleep = int(msg_value)
        except (ValueError, TypeError):
            seconds_sleep = 0
        await asyncio.sleep(seconds_sleep)
        print(f'FINISHED PROCESSING {request_id}: {msg_value}')

    @staticmethod
    def default_config_postgres_client():
        return PostgresClient(
            user='postgres',
            password='password',
            host='localhost',
            port=5434,
            db_name='postgres'
        )


@dataclass
class KafkaConsumer:
    consumer = None
    config: Dict
    topic: str

    def init(self):
        if not self.consumer:
            self.consumer = Consumer(self.config)
            log.info(f'[x] Kafka consumer initialized')

    def subscribe_to_topic(self):
        self.consumer.subscribe([self.topic])
        log.info(f'[x] Kafka consumer subscribed to {self.topic} topic')

    def assign_to_topic_partitions(self, partitions_num: int = 1):
        topic_partitions = [TopicPartition(self.topic, i) for i in range(partitions_num)]
        self.consumer.assign(
            topic_partitions
        )
        log.info(f'[x] Kafka consumer assigned to {partitions_num} partitions of {self.topic} topic')

    def subscribe(self):
        # self.subscribe_to_topic()
        self.assign_to_topic_partitions(partitions_num=1)

    async def poll(self, callback: Callable) -> None:
        while True:
            try:
                message = self.consumer.poll(0.01)
            except Exception as e:
                log.error(f'Polling exception occurred, reinitialising consumer. Exception:\n{str(e)}')
                self.init()
                message = None

            if message is None:
                pass
            elif message.error():
                log.warning(f'Received error message: {str(message.error())}')
            else:
                msg_value = message.value().decode()
                log.debug(f'[x] Received message with value: {msg_value}. Sending to Postgres...')
                asyncio.create_task(callback(msg_value=msg_value, request_id=random.randint(1, 999999999)))
            await asyncio.sleep(0.01)

    @staticmethod
    def default_config_consumer():
        return KafkaConsumer(
            config={
                'bootstrap.servers': 'localhost:9092',
                'group.id': 'test-group',
                'auto.offset.reset': 'latest',
                'enable.auto.commit': True,
                'auto.commit.interval.ms': 50
            },
            topic='test-topic'
        )


async def main():
    postgres_client = PostgresClient.default_config_postgres_client()

    postgres_client.create_engines()
    postgres_client.create_schemas(['autoclass', 'modorch'])
    postgres_client.create_all_tables(declarative_base_class=Base)

    kafka_consumer = KafkaConsumer.default_config_consumer()
    kafka_consumer.init()
    kafka_consumer.subscribe()

    await kafka_consumer.poll(callback=postgres_client.new_data_from_kafka)


if __name__ == '__main__':
    asyncio.run(main())
