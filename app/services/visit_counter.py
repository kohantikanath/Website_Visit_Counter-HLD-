 
from typing import Dict, List, Any
import asyncio
from datetime import datetime, timedelta
from ..core.redis_manager import RedisManager
from collections import defaultdict
 
class VisitCounterService:
 
    CACHE_TTL = 50
    BUFFER_FLUSH_INTERVAL = 30
 
    def __init__(self, redis_manager: RedisManager):
        """Initialize the visit counter service with Redis manager"""
        self.redis_manager = redis_manager
 
        # application layer cache for visit count
        self.visit_count_cache: Dict[str, Dict] = {}
        self.cache_locks = defaultdict(asyncio.Lock)
 
        # write buffer for visit count
        self.write_buffer = defaultdict(int)
        self.buffer_locks = defaultdict(asyncio.Lock)
 
        # start the buffer flush task
        asyncio.create_task(self.flush_buffer())
 
    async def flush_buffer(self) -> None:
        """
        Flush the write buffer to Redis
        """
        while True:
            await asyncio.sleep(VisitCounterService.BUFFER_FLUSH_INTERVAL)
            for page_id in list(self.write_buffer.keys()):
                await self.flush_buffer_key(page_id)
 
    async def flush_buffer_key(self, page_id: str) -> None:
        """
        Flush the write buffer to Redis for a specific key
        """
        if page_id not in self.write_buffer:
            return
 
        async with self.buffer_locks[page_id]:
            count = self.write_buffer[page_id]
            if count > 0:
                await self.redis_manager.increment(page_id, count)
            self.write_buffer.pop(page_id)
        self.buffer_locks.pop(page_id)
 
    def _cache_validity_check(self, page_id: str) -> bool:
        return (page_id in self.visit_count_cache and 
        (datetime.now() - self.visit_count_cache[page_id]["timestamp"]) < timedelta(seconds=self.CACHE_TTL))
 
    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        async with self.buffer_locks[page_id]:
            self.write_buffer[page_id] += 1
 
    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
 
        visit_count = 0
 
        if self._cache_validity_check(page_id):
            # using in-memory cache
            async with self.cache_locks[page_id]:
                visit_count = self.visit_count_cache[page_id]["count"]
        
        else:
            # flushing the data to redis before fetching the data
            await self.flush_buffer_key(page_id)
 
            # using redis cache 
            visit_count = await self.redis_manager.get(page_id)
            if visit_count is None:
                visit_count = 0
            # elif isinstance(visit_count, bytes):  
            #     visit_count = int(visit_count.decode()) 
 
            # update in-memory cache
            async with self.cache_locks[page_id]:
                self.visit_count_cache[page_id] = {
                    "count": visit_count,
                    "timestamp": datetime.now()
                }
        
        async with self.buffer_locks[page_id]:
            visit_count += self.write_buffer[page_id]
 
        return visit_count 