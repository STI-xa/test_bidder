import logging
import asyncio
import aiohttp
from sqlalchemy import Column, Integer
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from dotenv import load_dotenv
import os

load_dotenv()


DATABASE_URL = os.getenv('DATABASE_URL')


Base = declarative_base()


class Cpm(Base):
    '''param - subjectId;
    cpm - размер ставки;
    count - количество ставок;'''

    __tablename__ = 'cpm'

    id = Column(Integer, primary_key=True)
    cpm = Column(Integer)
    count = Column(Integer)
    param = Column(Integer)


async def get_db_session():
    engine = create_async_engine(DATABASE_URL)
    AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession)
    async with AsyncSessionLocal() as async_session:
        yield async_session


Session = sessionmaker(bind=get_db_session())

session = Session()


async def get_data(session, url, params):
    async with session.get(url, params=params) as response:
        data = await response.json()
        print(data)
        return data


async def save_data(session, data, param):
    try:
        bidder = Cpm(cpm=data['Cpm'], count=data['Count'], param=param)
        session.add(bidder)
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f'Возникла ошибка при сохранении данных: {str(e)}')


async def get_cmp(param, cpm, limit_cpm):
    try:
        db_bidder = session.query(Cpm).first()
        db_cpm = db_bidder.cpm

        if cpm <= db_cpm:
            cpm += 10
            if cpm > db_cpm:
                logging.info(
                    f'subjectId: {param}, cpm: {cpm} найдена нужная ставка'
                )
        elif cpm > limit_cpm:
            logging.warning('Достигнут лимит ставки')
    except Exception as e:
        logging.error(f'Что-то пошло не так, cpm: {str(e)}')


async def main():
    token = os.getenv('API_TOKEN')
    url = 'https://advert-api.wb.ru/adv/v0/cpm'
    param = 14430019
    limit_cpm = 1000

    params = {'type': 6, 'param': param}

    headers = {'Authorization': f'Bearer {token}'}
    async with aiohttp.ClientSession(headers=headers) as session:
        data = await get_data(session, url, params)
        await save_data(session, data, param)
        await get_cmp(param, data['Cmp'], limit_cpm)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
