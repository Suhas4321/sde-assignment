import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from uuid import uuid4
from src.services.token_budget import TokenBudgetManager
from src.models.customer_token_budget import CustomerTokenBudget


@pytest.mark.asyncio
async def test_get_customer_limits_cache_hit():
    """Test that limits are loaded from Redis cache directly on cache hit."""
    mock_redis = AsyncMock()
    mock_redis.get.side_effect = [b"30000", b"150"]

    manager = TokenBudgetManager(redis_conn=mock_redis)

    tpm, rpm = await manager.get_customer_limits("00000000-0000-0000-0000-000000000001")
    assert tpm == 30000
    assert rpm == 150
    assert mock_redis.get.call_count == 2


@pytest.mark.asyncio
async def test_get_customer_limits_cache_miss_db_hit():
    """Test that limits are loaded from Postgres and cached on cache miss."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None  # Cache miss
    mock_redis.set = AsyncMock()

    manager = TokenBudgetManager(redis_conn=mock_redis, cache_ttl=60)

    # Mock DB returns a CustomerTokenBudget row
    mock_budget = CustomerTokenBudget(
        customer_id=uuid4(),
        tokens_per_minute=45000,
        requests_per_minute=250,
        priority="premium"
    )

    with patch("src.services.token_budget.async_session_factory") as mock_session_factory:
        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        
        mock_scalars = MagicMock()
        mock_scalars.first = MagicMock(return_value=mock_budget)
        
        mock_result = MagicMock()
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        
        session_instance.execute = AsyncMock(return_value=mock_result)
        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        customer_uuid = str(uuid4())
        tpm, rpm = await manager.get_customer_limits(customer_uuid)

        assert tpm == 45000
        assert rpm == 250

        # Verify it was cached in Redis
        mock_redis.set.assert_any_call(f"token_budget:limit:tpm:{customer_uuid}", 45000, ex=60)
        mock_redis.set.assert_any_call(f"token_budget:limit:rpm:{customer_uuid}", 250, ex=60)


@pytest.mark.asyncio
async def test_check_and_reserve_budget_calls_eval():
    """Test that reserve budget executes Lua evaluation."""
    mock_redis = AsyncMock()
    mock_redis.eval = AsyncMock(return_value="customer")
    mock_redis.get.return_value = b"10000"  # Mock cache hits for limits

    manager = TokenBudgetManager(redis_conn=mock_redis)

    customer_uuid = str(uuid4())
    success, pool = await manager.check_and_reserve_budget(customer_uuid, 1500)

    assert success is True
    assert pool == "customer"
    mock_redis.eval.assert_called_once()


@pytest.mark.asyncio
async def test_release_budget_calls_eval():
    """Test that release budget executes Lua release decrement."""
    mock_redis = AsyncMock()
    mock_redis.eval = AsyncMock()

    manager = TokenBudgetManager(redis_conn=mock_redis)

    customer_uuid = str(uuid4())
    await manager.release_budget(customer_uuid, pool_used="customer", estimated_tokens=1500, actual_tokens=1200)

    # Release eval called for customer key with 300 tokens refund
    mock_redis.eval.assert_called_once()
    args = mock_redis.eval.call_args[0]
    assert args[2] == f"token_budget:used:tpm:{customer_uuid}"
    assert args[3] == 300
