import redis.asyncio as redis
import json
import os
from typing import Optional, Any

class RedisClient:
    def __init__(self):
        self.redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
        self.redis: Optional[redis.Redis] = None
        
    async def connect(self):
        """Подключается к Redis"""
        try:
            self.redis = redis.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            print("✅ Redis подключен")
        except Exception as e:
            print(f"❌ Ошибка подключения к Redis: {e}")
            self.redis = None
    
    async def cache_get(self, key: str) -> Optional[Any]:
        """Получает данные из кэша"""
        if not self.redis:
            return None
        try:
            data = await self.redis.get(key)
            return json.loads(data) if data else None
        except:
            return None
    
    async def cache_set(self, key: str, value: Any, expire: int = 3600):
        """Сохраняет данные в кэш"""
        if not self.redis:
            return
        try:
            await self.redis.setex(key, expire, json.dumps(value))
        except:
            pass
    
    async def increment_rate_limit(self, user_id: int, window: int = 60) -> int:
        """Увеличивает счетчик запросов для пользователя"""
        if not self.redis:
            return 0
            
        key = f"rate_limit:{user_id}"
        try:
            async with self.redis.pipeline() as pipe:
                pipe.incr(key)
                pipe.expire(key, window)
                results = await pipe.execute()
                return results[0]
        except:
            return 0
    
    async def get_rate_limit(self, user_id: int) -> int:
        """Получает текущий счетчик запросов"""
        if not self.redis:
            return 0
            
        key = f"rate_limit:{user_id}"
        try:
            count = await self.redis.get(key)
            return int(count) if count else 0
        except:
            return 0

# Глобальный экземпляр
redis_client = RedisClient()
