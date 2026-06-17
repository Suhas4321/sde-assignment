import abc
import time
import logging
from typing import Optional
import redis.asyncio as aioredis
from src.config import settings
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)


class AbstractRateLimiter(abc.ABC):
    """
    Abstract Base Class for LLM Rate Limiting.
    Defines the Strategy pattern interface.
    """

    @abc.abstractmethod
    async def acquire(self, tokens: int) -> bool:
        """
        Attempt to acquire capacity for an LLM call.
        Returns True if capacity is acquired, False otherwise.
        """
        pass

    @abc.abstractmethod
    async def release(self, estimated_tokens: int, actual_tokens: int) -> None:
        """
        Adjust capacity counter based on actual token usage vs estimated token usage.
        Useful to refund overestimated tokens to the bucket.
        """
        pass


class LocalMemoryRateLimiter(AbstractRateLimiter):
    """
    In-memory Rate Limiter for local testing or single-process systems.
    Uses token bucket algorithm.
    """

    def __init__(
        self,
        tpm_capacity: int = 90000,
        tpm_refill_rate: float = 1500.0,  # tokens per second (90000/60)
        rpm_capacity: int = 500,
        rpm_refill_rate: float = 8.33,   # requests per second (500/60)
    ):
        self.tpm_capacity = tpm_capacity
        self.tpm_refill_rate = tpm_refill_rate
        self.rpm_capacity = rpm_capacity
        self.rpm_refill_rate = rpm_refill_rate

        self.tpm_tokens = float(tpm_capacity)
        self.rpm_tokens = float(rpm_capacity)
        self.last_refill = time.time()

    def _refill(self) -> None:
        now = time.time()
        elapsed = max(0.0, now - self.last_refill)
        self.last_refill = now

        self.tpm_tokens = min(self.tpm_capacity, self.tpm_tokens + (elapsed * self.tpm_refill_rate))
        self.rpm_tokens = min(self.rpm_capacity, self.rpm_tokens + (elapsed * self.rpm_refill_rate))

    async def acquire(self, tokens: int) -> bool:
        self._refill()
        if self.tpm_tokens >= tokens and self.rpm_tokens >= 1:
            self.tpm_tokens -= tokens
            self.rpm_tokens -= 1
            if self.tpm_tokens < 0.2 * self.tpm_capacity:
                logger.warning(
                    "ALERT: Rate limit warning",
                    extra={
                        "tokens_available": self.tpm_tokens,
                        "tpm_capacity": self.tpm_capacity,
                        "usage_ratio": round((self.tpm_capacity - self.tpm_tokens) / self.tpm_capacity, 2)
                    }
                )
            return True
        return False

    async def release(self, estimated_tokens: int, actual_tokens: int) -> None:
        self._refill()
        refund = estimated_tokens - actual_tokens
        if refund > 0:
            self.tpm_tokens = min(self.tpm_capacity, self.tpm_tokens + refund)


class RedisTokenBucketLimiter(AbstractRateLimiter):
    """
    Distributed Redis-backed Rate Limiter.
    Uses atomic Lua scripts to prevent race conditions across multiple workers.
    """

    # Lua script to check and acquire tokens from both TPM and RPM buckets
    LUA_ACQUIRE = """
    local key_tpm_tokens = KEYS[1]
    local key_tpm_last = KEYS[2]
    local key_rpm_tokens = KEYS[3]
    local key_rpm_last = KEYS[4]

    local req_tpm_tokens = tonumber(ARGV[1])
    local tpm_capacity = tonumber(ARGV[2])
    local tpm_refill = tonumber(ARGV[3])
    local rpm_capacity = tonumber(ARGV[4])
    local rpm_refill = tonumber(ARGV[5])
    local now = tonumber(ARGV[6])

    -- Refill and fetch TPM
    local tpm_tokens = tonumber(redis.call('get', key_tpm_tokens) or tpm_capacity)
    local tpm_last = tonumber(redis.call('get', key_tpm_last) or now)
    local tpm_elapsed = math.max(0, now - tpm_last)
    local current_tpm = math.min(tpm_capacity, tpm_tokens + (tpm_elapsed * tpm_refill))

    -- Refill and fetch RPM
    local rpm_tokens = tonumber(redis.call('get', key_rpm_tokens) or rpm_capacity)
    local rpm_last = tonumber(redis.call('get', key_rpm_last) or now)
    local rpm_elapsed = math.max(0, now - rpm_last)
    local current_rpm = math.min(rpm_capacity, rpm_tokens + (rpm_elapsed * rpm_refill))

    -- Check if both are available
    if current_tpm >= req_tpm_tokens and current_rpm >= 1 then
        current_tpm = current_tpm - req_tpm_tokens
        current_rpm = current_rpm - 1

        redis.call('set', key_tpm_tokens, current_tpm)
        redis.call('set', key_tpm_last, now)
        redis.call('set', key_rpm_tokens, current_rpm)
        redis.call('set', key_rpm_last, now)
        return {1, current_tpm}
    else
        return {0, current_tpm}
    end
    """

    def __init__(
        self,
        redis_conn: aioredis.Redis,
        key_prefix: str = "rate_limit",
        tpm_capacity: int = 90000,
        tpm_refill_rate: float = 1500.0,  # 90000/60 per sec
        rpm_capacity: int = 500,
        rpm_refill_rate: float = 8.33,    # 500/60 per sec
    ):
        self.redis = redis_conn
        self.key_tpm_tokens = f"{key_prefix}:tpm:tokens"
        self.key_tpm_last = f"{key_prefix}:tpm:last"
        self.key_rpm_tokens = f"{key_prefix}:rpm:tokens"
        self.key_rpm_last = f"{key_prefix}:rpm:last"

        self.tpm_capacity = tpm_capacity
        self.tpm_refill_rate = tpm_refill_rate
        self.rpm_capacity = rpm_capacity
        self.rpm_refill_rate = rpm_refill_rate

    async def acquire(self, tokens: int) -> bool:
        now = time.time()
        try:
            res = await self.redis.eval(
                self.LUA_ACQUIRE,
                4,
                self.key_tpm_tokens,
                self.key_tpm_last,
                self.key_rpm_tokens,
                self.key_rpm_last,
                tokens,
                self.tpm_capacity,
                self.tpm_refill_rate,
                self.rpm_capacity,
                self.rpm_refill_rate,
                now,
            )
            if isinstance(res, list):
                success = bool(res[0])
                current_tpm = float(res[1])
            else:
                success = bool(res)
                current_tpm = self.tpm_capacity

            if current_tpm < 0.2 * self.tpm_capacity:
                logger.warning(
                    "ALERT: Rate limit warning",
                    extra={
                        "tokens_available": current_tpm,
                        "tpm_capacity": self.tpm_capacity,
                        "usage_ratio": round((self.tpm_capacity - current_tpm) / self.tpm_capacity, 2)
                    }
                )
            return success
        except Exception as e:
            # Fallback to true on redis failures to avoid blocking the pipeline completely
            logger.exception("redis_rate_limiter_error", extra={"error": str(e)})
            return True

    async def release(self, estimated_tokens: int, actual_tokens: int) -> None:
        refund = estimated_tokens - actual_tokens
        if refund <= 0:
            return

        now = time.time()
        try:
            # Safely refund overestimated tokens inside the bucket
            tpm_tokens = float(await self.redis.get(self.key_tpm_tokens) or self.tpm_capacity)
            tpm_last = float(await self.redis.get(self.key_tpm_last) or now)
            
            elapsed = max(0.0, now - tpm_last)
            current_tpm = min(self.tpm_capacity, tpm_tokens + (elapsed * self.tpm_refill_rate) + refund)
            
            await self.redis.set(self.key_tpm_tokens, current_tpm)
            await self.redis.set(self.key_tpm_last, now)
        except Exception as e:
            logger.exception("redis_rate_limiter_release_error", extra={"error": str(e)})


# Global production rate limiter instance using the shared redis connection
global_rate_limiter = RedisTokenBucketLimiter(
    redis_conn=redis_client,
    tpm_capacity=settings.LLM_TOKENS_PER_MINUTE,
    tpm_refill_rate=settings.LLM_TOKENS_PER_MINUTE / 60.0,
    rpm_capacity=settings.LLM_REQUESTS_PER_MINUTE,
    rpm_refill_rate=settings.LLM_REQUESTS_PER_MINUTE / 60.0,
)
