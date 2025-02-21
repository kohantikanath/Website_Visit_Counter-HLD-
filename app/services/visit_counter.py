from typing import Dict, List, Any
import asyncio
from datetime import datetime, timedelta
from ..core.redis_manager import RedisManager
from collections import defaultdict

class VisitCounterService:
    # visitCounter = defaultdict(int)
    # visitCounterLocks = defaultdict(asyncio.Lock)

    # Cache for storing visit count for a page
    cache_visit_count: Dict[str, Dict] = {}
    cache_locks = defaultdict(asyncio.Lock)
    ttl = 50
    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        self.redis_manager = RedisManager()

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        # async with VisitCounterService.visitCounterLocks[page_id]:
        #     VisitCounterService.visitCounter[page_id] += 1
        await self.redis_manager.increment(page_id,1)
        # TODO: Implement visit count increment
        pass

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        # async with VisitCounterService.visitCounterLocks[page_id]:
        #     return VisitCounterService.visitCounter[page_id]


        # 
        if page_id in VisitCounterService.cache_visit_count:
            if datetime.now() - VisitCounterService.cache_visit_count[page_id]['time'] < timedelta(seconds=VisitCounterService.ttl):

                async with VisitCounterService.cache_locks[page_id]:
                    return VisitCounterService.cache_visit_count[page_id]['count']


        visit_count = await self.redis_manager.get(page_id)
        if visit_count is None:
            return 0


        # update in-memory cache
        async with VisitCounterService.cache_locks[page_id]:
            VisitCounterService.cache_visit_count[page_id] = {'count': visit_count, 'time': datetime.now()}
            return visit_count
                
