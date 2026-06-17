import logging
from typing import Optional, Tuple
from uuid import UUID

import redis.asyncio as aioredis
from sqlalchemy import select, func
from src.config import settings
from src.utils.db import async_session_factory
from src.utils.redis_client import redis_client
from src.models.customer_token_budget import CustomerTokenBudget

logger = logging.getLogger(__name__)


class TokenBudgetManager:
    """
    Manages per-customer LLM capacity budgets and a global shared overflow pool.
    Implements Task 3D.
    """

    LUA_RESERVE = """
    local key_cust_tpm = KEYS[1]
    local key_cust_rpm = KEYS[2]
    local key_shared_tpm = KEYS[3]
    local key_shared_rpm = KEYS[4]

    local req_tpm = tonumber(ARGV[1])
    local limit_cust_tpm = tonumber(ARGV[2])
    local limit_cust_rpm = tonumber(ARGV[3])
    local limit_shared_tpm = tonumber(ARGV[4])
    local limit_shared_rpm = tonumber(ARGV[5])

    local cust_tpm_used = tonumber(redis.call('get', key_cust_tpm) or 0)
    local cust_rpm_used = tonumber(redis.call('get', key_cust_rpm) or 0)

    -- 1. Check customer allocation
    if (cust_tpm_used + req_tpm <= limit_cust_tpm) and (cust_rpm_used + 1 <= limit_cust_rpm) then
        redis.call('incrby', key_cust_tpm, req_tpm)
        redis.call('incr', key_cust_rpm)
        if cust_tpm_used == 0 then
            redis.call('expire', key_cust_tpm, 60)
        end
        if cust_rpm_used == 0 then
            redis.call('expire', key_cust_rpm, 60)
        end
        return "customer"
    end

    -- 2. Fallback to shared pool
    local shared_tpm_used = tonumber(redis.call('get', key_shared_tpm) or 0)
    local shared_rpm_used = tonumber(redis.call('get', key_shared_rpm) or 0)

    if (shared_tpm_used + req_tpm <= limit_shared_tpm) and (shared_rpm_used + 1 <= limit_shared_rpm) then
        redis.call('incrby', key_shared_tpm, req_tpm)
        redis.call('incr', key_shared_rpm)
        if shared_tpm_used == 0 then
            redis.call('expire', key_shared_tpm, 60)
        end
        if shared_rpm_used == 0 then
            redis.call('expire', key_shared_rpm, 60)
        end
        return "shared"
    end

    return "exhausted"
    """

    LUA_RELEASE = """
    local key = KEYS[1]
    local amount = tonumber(ARGV[1])
    local current = tonumber(redis.call('get', key) or 0)
    if current > 0 then
        local new_val = math.max(0, current - amount)
        redis.call('set', key, new_val, 'KEEPTTL')
    end
    return 1
    """

    def __init__(
        self,
        redis_conn: aioredis.Redis,
        cache_ttl: int = 300,
        default_tpm: int = 15000,
        default_rpm: int = 100,
    ):
        self.redis = redis_conn
        self.cache_ttl = cache_ttl
        self.default_tpm = default_tpm
        self.default_rpm = default_rpm

    async def get_customer_limits(self, customer_id: str) -> Tuple[int, int]:
        """
        Retrieve customer token and request limits, checking Redis cache first,
        then Postgres database, and falling back to default configuration.
        """
        key_tpm = f"token_budget:limit:tpm:{customer_id}"
        key_rpm = f"token_budget:limit:rpm:{customer_id}"

        try:
            cached_tpm = await self.redis.get(key_tpm)
            cached_rpm = await self.redis.get(key_rpm)

            if cached_tpm is not None and cached_rpm is not None:
                return int(cached_tpm), int(cached_rpm)
        except Exception as e:
            logger.warning(f"Redis cache budget read failure: {e}")

        # Cache miss: Load from Postgres
        tpm, rpm = self.default_tpm, self.default_rpm
        try:
            async with async_session_factory() as session:
                stmt = select(CustomerTokenBudget).where(
                    CustomerTokenBudget.customer_id == UUID(customer_id),
                    CustomerTokenBudget.is_active == True
                )
                res = await session.execute(stmt)
                budget = res.scalars().first()
                if budget:
                    tpm = budget.tokens_per_minute
                    rpm = budget.requests_per_minute
        except Exception as e:
            logger.warning(f"Database budget lookup failure: {e}. Falling back to default.")

        # Cache the results
        try:
            await self.redis.set(key_tpm, tpm, ex=self.cache_ttl)
            await self.redis.set(key_rpm, rpm, ex=self.cache_ttl)
        except Exception as e:
            logger.warning(f"Failed to cache customer limits in Redis: {e}")

        return tpm, rpm

    async def get_shared_pool_limits(self) -> Tuple[int, int]:
        """
        Calculate and return shared pool limits (total capacity minus allocated guaranteed allocations).
        """
        key_tpm = "token_budget:limit:tpm:shared"
        key_rpm = "token_budget:limit:rpm:shared"

        try:
            cached_tpm = await self.redis.get(key_tpm)
            cached_rpm = await self.redis.get(key_rpm)

            if cached_tpm is not None and cached_rpm is not None:
                return int(cached_tpm), int(cached_rpm)
        except Exception as e:
            logger.warning(f"Redis cache shared limits read failure: {e}")

        # Cache miss: Load allocation sum from Postgres
        total_allocated_tpm = 0
        total_allocated_rpm = 0
        try:
            async with async_session_factory() as session:
                stmt_tpm = select(func.sum(CustomerTokenBudget.tokens_per_minute)).where(
                    CustomerTokenBudget.is_active == True
                )
                res_tpm = await session.execute(stmt_tpm)
                total_allocated_tpm = res_tpm.scalar() or 0

                stmt_rpm = select(func.sum(CustomerTokenBudget.requests_per_minute)).where(
                    CustomerTokenBudget.is_active == True
                )
                res_rpm = await session.execute(stmt_rpm)
                total_allocated_rpm = res_rpm.scalar() or 0
        except Exception as e:
            logger.warning(f"Database allocations sum lookup failure: {e}.")

        # Shared limits = Global Settings limits - Pre-allocated limits
        shared_tpm = max(15000, settings.LLM_TOKENS_PER_MINUTE - total_allocated_tpm)
        shared_rpm = max(100, settings.LLM_REQUESTS_PER_MINUTE - total_allocated_rpm)

        try:
            await self.redis.set(key_tpm, shared_tpm, ex=self.cache_ttl)
            await self.redis.set(key_rpm, shared_rpm, ex=self.cache_ttl)
        except Exception as e:
            logger.warning(f"Failed to cache shared limits in Redis: {e}")

        return shared_tpm, shared_rpm

    async def check_and_reserve_budget(self, customer_id: str, estimated_tokens: int) -> Tuple[bool, str]:
        """
        Atomically check and reserve the requested tokens under the customer allocation
        or fallback shared pool.
        Returns a tuple of (is_successful, pool_used: "customer" | "shared" | "exhausted").
        """
        tpm_limit, rpm_limit = await self.get_customer_limits(customer_id)
        shared_tpm_limit, shared_rpm_limit = await self.get_shared_pool_limits()

        key_cust_tpm = f"token_budget:used:tpm:{customer_id}"
        key_cust_rpm = f"token_budget:used:rpm:{customer_id}"
        key_shared_tpm = "token_budget:used:tpm:shared"
        key_shared_rpm = "token_budget:used:rpm:shared"

        try:
            pool_used = await self.redis.eval(
                self.LUA_RESERVE,
                4,
                key_cust_tpm,
                key_cust_rpm,
                key_shared_tpm,
                key_shared_rpm,
                estimated_tokens,
                tpm_limit,
                rpm_limit,
                shared_tpm_limit,
                shared_rpm_limit,
            )
            # Eval returns strings in Python redis if matched
            if pool_used in ("customer", "shared"):
                return True, pool_used

            logger.warning(
                "ALERT: Customer budget exceeded",
                extra={
                    "customer_id": customer_id,
                    "estimated_tokens": estimated_tokens,
                    "tpm_limit": tpm_limit,
                    "shared_tpm_limit": shared_tpm_limit,
                }
            )
            return False, "exhausted"
        except Exception as e:
            logger.exception("redis_token_budget_eval_error", extra={"error": str(e)})
            # Resiliency: Fallback to allowing requests on redis connection errors
            return True, "customer"

    async def release_budget(
        self, customer_id: str, pool_used: str, estimated_tokens: int, actual_tokens: int
    ) -> None:
        """
        Refund the difference if actual token usage is lower than the estimation.
        """
        refund = estimated_tokens - actual_tokens
        if refund <= 0 or pool_used not in ("customer", "shared"):
            return

        if pool_used == "customer":
            key = f"token_budget:used:tpm:{customer_id}"
        else:
            key = "token_budget:used:tpm:shared"

        try:
            await self.redis.eval(self.LUA_RELEASE, 1, key, refund)
        except Exception as e:
            logger.exception("redis_token_budget_release_eval_error", extra={"error": str(e)})


token_budget_manager = TokenBudgetManager(redis_conn=redis_client)
