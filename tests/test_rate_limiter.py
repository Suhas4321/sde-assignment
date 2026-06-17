import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from src.services.rate_limiter import LocalMemoryRateLimiter, RedisTokenBucketLimiter


@pytest.mark.asyncio
async def test_local_rate_limiter_acquires_tokens():
    """Test that LocalMemoryRateLimiter correctly throttles when capacity is exhausted."""
    # Capacity: 3000 tokens, 50 tokens/sec refill rate. RPM capacity: 2, 1 req/sec refill rate.
    limiter = LocalMemoryRateLimiter(
        tpm_capacity=3000,
        tpm_refill_rate=50.0,
        rpm_capacity=2,
        rpm_refill_rate=1.0
    )

    # First attempt: acquire 1500 tokens. Success.
    assert await limiter.acquire(1500) is True
    assert limiter.tpm_tokens == pytest.approx(1500, abs=0.5)
    assert limiter.rpm_tokens == pytest.approx(1, abs=0.1)

    # Second attempt: acquire 1500 tokens. Success.
    assert await limiter.acquire(1500) is True
    assert limiter.tpm_tokens == pytest.approx(0, abs=0.5)
    assert limiter.rpm_tokens == pytest.approx(0, abs=0.1)

    # Third attempt: acquire 1500 tokens. Failed (no tokens left and RPM is 0).
    assert await limiter.acquire(1500) is False


@pytest.mark.asyncio
async def test_local_rate_limiter_refills_over_time():
    """Test that LocalMemoryRateLimiter refills capacity after sleeping/waiting."""
    limiter = LocalMemoryRateLimiter(
        tpm_capacity=1000,
        tpm_refill_rate=1000.0,  # Refills fully in 1 second
        rpm_capacity=10,
        rpm_refill_rate=10.0
    )

    assert await limiter.acquire(1000) is True
    assert await limiter.acquire(1) is False  # Exhausted

    # Simulate waiting 0.5 seconds
    limiter.last_refill -= 0.5

    # Refilled 500 tokens
    assert await limiter.acquire(500) is True
    assert await limiter.acquire(10) is False  # Exhausted again


@pytest.mark.asyncio
async def test_local_rate_limiter_refund():
    """Test that LocalMemoryRateLimiter correctly refunds overestimated tokens."""
    limiter = LocalMemoryRateLimiter(
        tpm_capacity=1000,
        tpm_refill_rate=10.0,
        rpm_capacity=10,
        rpm_refill_rate=1.0
    )

    # Acquire 1000 tokens (exhausted)
    assert await limiter.acquire(1000) is True
    assert await limiter.acquire(1) is False

    # Release and refund 400 tokens
    await limiter.release(estimated_tokens=1000, actual_tokens=600)

    # We should be able to acquire up to 400 tokens now
    assert await limiter.acquire(400) is True
    assert await limiter.acquire(1) is False


@pytest.mark.asyncio
async def test_redis_rate_limiter_calls_eval():
    """Test that RedisTokenBucketLimiter correctly invokes redis.eval with correct arguments."""
    mock_redis = AsyncMock()
    mock_redis.eval = AsyncMock(return_value=1)

    limiter = RedisTokenBucketLimiter(
        redis_conn=mock_redis,
        key_prefix="test_limiter",
        tpm_capacity=5000,
        tpm_refill_rate=100.0,
        rpm_capacity=5,
        rpm_refill_rate=1.0,
    )

    # Acquire returns True because mock_redis.eval returns 1
    assert await limiter.acquire(1500) is True

    # Verify that eval was called with correct keys and args
    mock_redis.eval.assert_called_once()
    args = mock_redis.eval.call_args[0]
    
    # args[0] is LUA_ACQUIRE
    # args[1] is number of keys (4)
    # args[2:6] are keys: tpm_tokens, tpm_last, rpm_tokens, rpm_last
    assert args[1] == 4
    assert args[2] == "test_limiter:tpm:tokens"
    assert args[3] == "test_limiter:tpm:last"
    assert args[4] == "test_limiter:rpm:tokens"
    assert args[5] == "test_limiter:rpm:last"
    
    # ARGV values: requested_tokens (1500), tpm_capacity (5000), tpm_refill_rate (100.0), etc.
    assert args[6] == 1500
    assert args[7] == 5000
    assert args[8] == 100.0
