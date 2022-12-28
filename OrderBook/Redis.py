import redis
import os

REDIS_HOST = os.environ['REDIS_HOST'] if 'REDIS_HOST' in os.environ else 'localhost'
REDIS_PORT = os.environ['REDIS_PORT'] if 'REDIS_PORT' in os.environ else 6379
REDIS_PASSWORD = os.environ['REDIS_PASSWORD'] if 'REDIS_PASSWORD' in os.environ else ''

_redis = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, password=REDIS_PASSWORD)