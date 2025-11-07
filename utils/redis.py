import redis.asyncio as redis

redis_url = str("some-redis-uri")
redis_pool = redis.ConnectionPool.from_url(
    redis_url,
    decode_responses=True,
)
redis_client = redis.Redis(connection_pool=redis_pool)
