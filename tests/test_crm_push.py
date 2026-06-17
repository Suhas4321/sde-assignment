import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock
from uuid import uuid4
import httpx
from src.services.customer_config_manager import CustomerConfigManager
from src.tasks.celery_tasks import _process_crm_push
from src.models.processing_task import TaskPriority, TaskStatus


@pytest.mark.asyncio
async def test_customer_config_cache_hit():
    """Test that customer config is retrieved from cache if present."""
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=json.dumps({
        "hot_keywords": ["urg", "now"],
        "crm_webhook_url": "https://crm.cache.com/hook"
    }))
    
    manager = CustomerConfigManager(redis_conn=mock_redis)
    customer_id = str(uuid4())
    
    keywords, url = await manager.get_config(customer_id)
    assert keywords == ["urg", "now"]
    assert url == "https://crm.cache.com/hook"
    mock_redis.get.assert_called_once_with(f"customer_config:{customer_id}")


@pytest.mark.asyncio
async def test_customer_config_cache_miss_db_hit():
    """Test that cache miss triggers Postgres query and writes back to Redis cache."""
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock()

    # Mock DB Config model
    mock_config = MagicMock()
    mock_config.hot_keywords = ["db_keyword"]
    mock_config.crm_webhook_url = "https://db.crm.com/hook"

    with patch("src.services.customer_config_manager.async_session_factory") as mock_session_factory:
        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        
        mock_result = MagicMock()
        mock_result.scalars = MagicMock(return_value=MagicMock(first=MagicMock(return_value=mock_config)))
        
        session_instance.execute = AsyncMock(return_value=mock_result)
        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        manager = CustomerConfigManager(redis_conn=mock_redis)
        customer_id = str(uuid4())

        keywords, url = await manager.get_config(customer_id)
        assert keywords == ["db_keyword"]
        assert url == "https://db.crm.com/hook"
        
        mock_redis.set.assert_called_once()
        set_args = mock_redis.set.call_args[0]
        assert set_args[0] == f"customer_config:{customer_id}"
        assert "db_keyword" in set_args[1]


@pytest.mark.asyncio
async def test_crm_push_success():
    """Test successful CRM webhook push transitions task to completed status."""
    task_id = str(uuid4())
    interaction_uuid = str(uuid4())
    customer_uuid = str(uuid4())
    campaign_uuid = str(uuid4())

    mock_pt = MagicMock()
    mock_pt.interaction_id = interaction_uuid
    mock_pt.customer_id = customer_uuid
    mock_pt.campaign_id = campaign_uuid
    mock_pt.attempt_count = 0
    mock_pt.payload = {
        "crm_webhook_url": "https://activecrm.com/hook",
        "analysis_result": {"call_stage": "demo_booked"}
    }

    with patch("src.tasks.celery_tasks.async_session_factory") as mock_session_factory, \
         patch("src.tasks.celery_tasks.task_manager") as mock_tm, \
         patch("src.tasks.celery_tasks.log_event", new_callable=AsyncMock) as mock_log, \
         patch("httpx.AsyncClient.post") as mock_post:

        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.scalars = MagicMock(return_value=MagicMock(first=MagicMock(return_value=mock_pt)))
        session_instance.execute = AsyncMock(return_value=mock_result)
        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        mock_tm.claim_task = AsyncMock(return_value=True)
        mock_tm.complete_task = AsyncMock()

        # Mock successful webhook call
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp

        await _process_crm_push(None, task_id)

        mock_post.assert_called_once_with("https://activecrm.com/hook", json={"call_stage": "demo_booked"})
        mock_tm.complete_task.assert_called_once_with(task_id, {"status": "success", "status_code": 200})
        mock_log.assert_any_call(
            interaction_id=interaction_uuid,
            event_type="crm_push_success",
            step="crm_push",
            status="completed",
            customer_id=customer_uuid,
            campaign_id=campaign_uuid,
            metadata={"status": "success", "status_code": 200}
        )


@pytest.mark.asyncio
async def test_crm_push_skipped_when_no_url():
    """Test that crm push task is completed as skipped if no webhook URL is configured."""
    task_id = str(uuid4())
    interaction_uuid = str(uuid4())
    customer_uuid = str(uuid4())
    campaign_uuid = str(uuid4())

    mock_pt = MagicMock()
    mock_pt.interaction_id = interaction_uuid
    mock_pt.customer_id = customer_uuid
    mock_pt.campaign_id = campaign_uuid
    mock_pt.attempt_count = 0
    mock_pt.payload = {
        "analysis_result": {"call_stage": "not_interested"}
    }

    with patch("src.tasks.celery_tasks.async_session_factory") as mock_session_factory, \
         patch("src.tasks.celery_tasks.task_manager") as mock_tm, \
         patch("src.services.customer_config_manager.customer_config_manager") as mock_config, \
         patch("src.tasks.celery_tasks.log_event", new_callable=AsyncMock) as mock_log, \
         patch("httpx.AsyncClient.post") as mock_post:

        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.scalars = MagicMock(return_value=MagicMock(first=MagicMock(return_value=mock_pt)))
        session_instance.execute = AsyncMock(return_value=mock_result)
        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        mock_tm.claim_task = AsyncMock(return_value=True)
        mock_tm.complete_task = AsyncMock()
        mock_config.get_config = AsyncMock(return_value=([], None))  # Cache/DB returns no URL

        await _process_crm_push(None, task_id)

        mock_post.assert_not_called()
        mock_tm.complete_task.assert_called_once_with(task_id, {"status": "skipped", "reason": "no_webhook_url"})


@pytest.mark.asyncio
async def test_crm_push_propagates_http_error():
    """Test that HTTP errors from the webhook endpoint bubble up so the task manager retry is fired."""
    task_id = str(uuid4())
    interaction_uuid = str(uuid4())
    customer_uuid = str(uuid4())
    campaign_uuid = str(uuid4())

    mock_pt = MagicMock()
    mock_pt.interaction_id = interaction_uuid
    mock_pt.customer_id = customer_uuid
    mock_pt.campaign_id = campaign_uuid
    mock_pt.attempt_count = 0
    mock_pt.payload = {
        "crm_webhook_url": "https://failingcrm.com/hook",
        "analysis_result": {"call_stage": "rebook_confirmed"}
    }

    with patch("src.tasks.celery_tasks.async_session_factory") as mock_session_factory, \
         patch("src.tasks.celery_tasks.task_manager") as mock_tm, \
         patch("src.tasks.celery_tasks.log_event", new_callable=AsyncMock) as mock_log, \
         patch("httpx.AsyncClient.post") as mock_post:

        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.scalars = MagicMock(return_value=MagicMock(first=MagicMock(return_value=mock_pt)))
        session_instance.execute = AsyncMock(return_value=mock_result)
        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        mock_tm.claim_task = AsyncMock(return_value=True)

        # Mock HTTP Failure
        mock_resp = httpx.Response(500, request=httpx.Request("POST", "https://failingcrm.com/hook"))
        mock_post.return_value = mock_resp

        with pytest.raises(httpx.HTTPStatusError):
            await _process_crm_push(None, task_id)
        
        mock_post.assert_called_once()
