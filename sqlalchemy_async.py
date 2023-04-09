import asyncio
from dataclasses import dataclass
import logging
import random
import time
import datetime

from sqlalchemy import Column, BigInteger, Text, DateTime, select
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.exc import NoResultFound, MultipleResultsFound


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
class AsyncPostgresClient:
    user: str
    password: str
    host: str
    port: int
    db_name: str

    _engine = None
    _session = None

    def init(self):
        url = f'postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}'
        self._engine = create_async_engine(url)
        self._engine.connect()

    async def drop_all_tables(self, declarative_base_class):
        async with self._engine.begin() as conn:
            await conn.run_sync(declarative_base_class.metadata.drop_all)

    async def create_all_tables(self, declarative_base_class):
        async with self._engine.begin() as conn:
            await conn.run_sync(declarative_base_class.metadata.create_all)

    async def drop_and_create_all_tables(self, declarative_base_class):
        await self.drop_all_tables(declarative_base_class)
        await self.create_all_tables(declarative_base_class)

    async def process_new_session(self, session_id: str):
        print(f'Checking session_id in database...')
        async with AsyncSession(self._engine) as session:
            result = await session.execute(select(Sessions).where(Sessions.session_id == session_id))
            try:
                result.scalars().one()
                print('ONE FOUND')
            except MultipleResultsFound:
                print('MANY FOUND')
            except NoResultFound:
                print('NOT FOUND')
                print(f'Writing data to Postgres with session id: {session_id}...')
                data_to_add = [Sessions(session_id=session_id, session_datetime=datetime.datetime.now()) for _ in
                               range(1_000)]
                async with AsyncSession(self._engine) as session:
                    async with session.begin():
                        session.add_all(
                            data_to_add
                        )
                    await session.commit()
                print(f'[x] {session_id} data saved to DB')
            except Exception:
                raise

    @staticmethod
    def default_config_postgres_client():
        return AsyncPostgresClient(
            user='postgres',
            password='password',
            host='localhost',
            port=5434,
            db_name='postgres'
        )


async def main():
    async_postgres_client = AsyncPostgresClient.default_config_postgres_client()

    async_postgres_client.init()
    await async_postgres_client.drop_and_create_all_tables(declarative_base_class=Base)

    while True:
        print(f'{time.time()} Waiting for data...')
        session_id = random.randint(1, 5)
        print(f'[x] NEW SESSION: {session_id}')
        asyncio.create_task(async_postgres_client.process_new_session(session_id=str(session_id)))
        await asyncio.sleep(0.5)


if __name__ == '__main__':
    asyncio.run(main())
