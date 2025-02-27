import redis
from typing import Dict, List, Optional, Any
from .consistent_hash import ConsistentHash
from .config import settings
import os

class RedisManager:
    def __init__(self):
        """Initialize Redis connection pools and consistent hashing"""
        # self.connection_pools: Dict[str, redis.ConnectionPool] = {}
        # self.redis_clients: Dict[str, redis.Redis] = {}
        redisUrl = os.getenv('REDIS_NODES').split(',')[0]
        self.connection_pool = redis.ConnectionPool.from_url(redisUrl)
        
        # Parse Redis nodes from comma-separated string
        # redis_nodes = [node.strip() for node in settings.REDIS_NODES.split(",") if node.strip()]
        # self.consistent_hash = ConsistentHash(redis_nodes, settings.VIRTUAL_NODES)
        
        # TODO: Initialize connection pools for each Redis node
        # 1. Create connection pools for each Redis node
        # 2. Initialize Redis clients
        pass

    async def get_connection(self, key: str) -> redis.Redis:
        """
        Get Redis connection for the given key using consistent hashing
        
        Args:
            key: The key to determine which Redis node to use
            
        Returns:
            Redis client for the appropriate node
        """
        redis_client = redis.Redis(connection_pool=self.connection_pool)
        return redis_client
        # TODO: Implement getting the appropriate Redis connection
        # 1. Use consistent hashing to determine which node should handle this key
        # 2. Return the Redis client for that node
        pass

    async def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a counter in Redis
        
        Args:
            key: The key to increment
            amount: Amount to increment by
            
        Returns:
            New value of the counter
        """
        # TODO: Implement incrementing a counter
        # 1. Get the appropriate Redis connection
        # 2. Increment the counter
        # 3. Handle potential failures and retries
        redis_client = await self.get_connection(key)
        return redis_client.incr(key, amount)

    async def get(self, key: str) -> Optional[int]:
        """
        Get value for a key from Redis
        
        Args:
            key: The key to get
            
        Returns:
            Value of the key or None if not found
        """
        # TODO: Implement getting a value
        # 1. Get the appropriate Redis connection
        # 2. Retrieve the value
        # 3. Handle potential failures and retries
        redis_client = await self.get_connection(key)
        value = redis_client.get(key)
        return int(value) if value else 0
