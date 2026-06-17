import json
import logging
from typing import List, Optional, Tuple
from uuid import UUID

import redis.asyncio as aioredis
from sqlalchemy import select
from src.utils.db import async_session_factory
from src.utils.redis_client import redis_client
from src.models.customer_config import CustomerConfig

logger = logging.getLogger(__name__)


class CustomerConfigManager:
    """
    Manages loading and caching of per-customer campaign configurations (e.g. webhook URLs, hot keywords).
    """

    def __init__(
        self,
        redis_conn: aioredis.Redis,
        cache_ttl: int = 300,
        default_hot_keywords: Optional[List[str]] = None,
    ):
        self.redis = redis_conn
        self.cache_ttl = cache_ttl
        self.default_hot_keywords = default_hot_keywords or [
            "confirmed", "booked", "scheduled", "appointment",
            "manager", "escalate", "complaint", "urgent",
            "demo", "meeting", "canceled", "refund"
        ]

    async def get_config(self, customer_id: str) -> Tuple[List[str], Optional[str]]:
        """
        Retrieves the hot keywords and CRM webhook URL for a customer, checking cache first,
        then falling back to the database.
        Returns a tuple of (hot_keywords, crm_webhook_url).
        """
        key = f"customer_config:{customer_id}"

        # 1. Read from Redis Cache
        try:
            cached = await self.redis.get(key)
            if cached is not None:
                data = json.loads(cached)
                return data["hot_keywords"], data.get("crm_webhook_url")
        except Exception as e:
            logger.warning(f"Failed to read customer config cache: {e}")

        # 2. Database Fallback
        hot_keywords = self.default_hot_keywords
        crm_webhook_url = None

        try:
            async with async_session_factory() as session:
                stmt = select(CustomerConfig).where(CustomerConfig.customer_id == UUID(customer_id))
                res = await session.execute(stmt)
                config = res.scalars().first()
                if config:
                    hot_keywords = config.hot_keywords
                    crm_webhook_url = config.crm_webhook_url
        except Exception as e:
            logger.warning(f"Database lookup for customer config {customer_id} failed: {e}")

        # 3. Write back to Cache
        try:
            await self.redis.set(
                key,
                json.dumps({
                    "hot_keywords": hot_keywords,
                    "crm_webhook_url": crm_webhook_url
                }),
                ex=self.cache_ttl
            )
        except Exception as e:
            logger.warning(f"Failed to cache customer config: {e}")

        return hot_keywords, crm_webhook_url


customer_config_manager = CustomerConfigManager(redis_conn=redis_client)
