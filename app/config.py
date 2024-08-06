from sqlalchemy import create_engine, MetaData, text
from databases import Database
from tenacity import retry, wait_fixed, stop_after_attempt
from asyncio import sleep
import redis.asyncio as redis
from .env import DATABASE_URL

database = Database(DATABASE_URL)
metadata = MetaData()
redis_client = None
# Database engine
engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_recycle=1800,  # recycle connections every 30 minutes
    pool_pre_ping=True,  # test connections before using them
)

metadata.create_all(engine)

@retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
async def connect():
    await database.connect()

@retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
async def disconnect():
    await database.disconnect()

async def init_redis():
    global redis_client
    redis_client = redis.Redis.from_url('redis://localhost', decode_responses=True)
    #print("Redis client initialized:", redis_client)

async def close_redis():
    global redis_client
    await redis_client.close()

def get_redis():
    return redis_client