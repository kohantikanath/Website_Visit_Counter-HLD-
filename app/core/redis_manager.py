 
import os
import redis
from typing import Dict, List, Tuple, Any, Optional
from .consistent_hash import ConsistentHash
from .config import settings
from bisect import bisect_left
 
class RedisManager:
 
    MAX_POOL_CONNECTIONS = 200
 
    def __init__(self):
        """Initialize Redis connection pools and consistent hashing"""
        redis_urls = os.getenv("REDIS_NODES").split(",") if os.getenv("REDIS_NODES") else ["redis://redis1:6379"]
        self.connection_pools: Dict[str, redis.ConnectionPool] = {}
        self.redis_clients: Dict[str, redis.Redis] = {}
        self.consistent_hash = ConsistentHash()
 
        for redis_url in redis_urls:
            self.add_redis_instance(redis_url)
    
    def add_redis_instance(self, redis_url: str) -> None:
        """
        Add a new Redis instance to the manager
        """
 
        if redis_url in self.redis_clients:
            return
 
        print(f"Adding Redis instance: {redis_url}")
 
        # creating redis connection pool and redis client
        connection_pool = redis.ConnectionPool.from_url(redis_url, decode_responses=True, max_connections=RedisManager.MAX_POOL_CONNECTIONS)
        self.connection_pools[redis_url] = connection_pool
        self.redis_clients[redis_url] = redis.Redis(connection_pool=connection_pool)
 
        # getting old state before adding node
        old_keys = self.consistent_hash.sorted_keys.copy()
        old_hash_ring = self.consistent_hash.hash_ring.copy()
 
        # adding node to consistent hash
        self.consistent_hash.add_node(redis_url)
 
        # getting all keys except the ones in the new node
        # assuming that the new instance is empty
        all_keys = list ( set(self.get_all_keys()) - set(self.redis_clients[redis_url].keys()) )
 
 
        print(f"Keys: {all_keys}")
 
        for key in all_keys:
            # if node is not the new node, we can skip
            node = self.consistent_hash.get_node(key)
            if node != redis_url:
                continue
 
            # figuring out the old node for the key
            hash_value = self.consistent_hash._hash(key)
            old_node = old_hash_ring[old_keys[bisect_left(old_keys,hash_value) % len(old_hash_ring)]]
 
            print(f"Key: {key} is being migrated from {old_node} to {node}")
            
            # migration of key from old node to new node
            value = self.redis_clients[old_node].get(key)
            self.redis_clients[node].set(key, value)
            self.redis_clients[old_node].delete(key)
 
 
 
    def remove_redis_instance(self, redis_url: str) -> None:
        """
        Remove a Redis instance from the manager
        """
 
        if redis_url not in self.redis_clients:
            return
 
        if len(self.redis_clients) == 1:
            print("Cannot remove the last Redis instance")
            return
 
        print(f"Removing Redis instance: {redis_url}")
 
        # getting old state before removing node
        old_keys = self.consistent_hash.sorted_keys.copy()
        old_hash_ring = self.consistent_hash.hash_ring.copy()
        
        # remove the node from consistent hashing
        self.consistent_hash.remove_node(redis_url)
        
        # get all keys currently in the Redis instance being removed
        all_keys = self.redis_clients[redis_url].keys()
        
        print(f"Keys: {all_keys}")
 
        for key in all_keys:
            new_node = self.consistent_hash.get_node(key)
            print(f"Key: {key} is being migrated from {redis_url} to {new_node}")
 
            # migration of key from old node to new node
            value = self.redis_clients[redis_url].get(key)
            self.redis_clients[new_node].set(key, value)
            self.redis_clients[redis_url].delete(key)
        
        # Remove Redis instance from manager
        self.redis_clients.pop(redis_url)
        self.connection_pools.pop(redis_url)
 
    def get_all_keys(self) -> List[str]:
        """
        Get all keys from all Redis instances
        
        Returns:
            List of all keys
        """
        all_keys = []
        for redis_client in self.redis_clients.values():
            all_keys += redis_client.keys()
        return all_keys
 
    def get_redis_node_from_key(self, key: str) -> str:
        """
        Get Redis node for the given key
        
        Args:
            key: The key to determine which Redis node to use
            
        Returns:
            Redis node for the key
        """
        return self.consistent_hash.get_node(key)
 
    def get_connection(self, key: str) -> redis.Redis:
        """
        Get Redis connection for the given key using consistent hashing
        
        Args:
            key: The key to determine which Redis node to use
            
        Returns:
            Redis client for the appropriate node
        """
        node = self.consistent_hash.get_node(key)
        if node is None:
            raise Exception("No Redis nodes available")
        return self.redis_clients[node]
 
    async def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """
        Increment a counter in Redis
        
        Args:
            key: The key to increment
            amount: Amount to increment by
            
        Returns:
            New value of the counter
        """
        redis_client = self.get_connection(key)
        return redis_client.incr(key, amount)
 
    async def get(self, key: str) -> Optional[int]:
        """
        Get value for a key from Redis
        
        Args:
            key: The key to get
            
        Returns:
            Value of the key or None if not found
        """
        redis_client = self.get_connection(key)
        value = redis_client.get(key)
 
        return int(value) if value is not None else None
 
 