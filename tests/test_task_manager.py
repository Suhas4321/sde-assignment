import pytest
from datetime import datetime
from unittest.mock import AsyncMock, patch, MagicMock
from uuid import uuid4
from src.services.task_manager import task_manager
from src.models.processing_task import TaskStatus, TaskPriority


@pytest.fixture(autouse=True)
def mock_trigger_alert_checks():
    with patch("src.services.task_manager.trigger_alert_checks", new_callable=AsyncMock) as mock:
        yield mock



@pytest.mark.asyncio
async def test_create_task():
    """Test that task_manager.create_task inserts a new task row into database."""
    with patch("src.services.task_manager.async_session_factory") as mock_session_factory:
        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        session_instance.commit = AsyncMock()
        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        interaction_id = str(uuid4())
        customer_id = str(uuid4())
        campaign_id = str(uuid4())

        task_id = await task_manager.create_task(
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            task_type="llm_analysis",
            priority="hot",
            payload={"test": "data"},
            max_attempts=3
        )

        assert task_id is not None
        session_instance.add.assert_called_once()
        session_instance.commit.assert_called_once()

        # Check model construction args
        added_task = session_instance.add.call_args[0][0]
        assert str(added_task.interaction_id) == interaction_id
        assert str(added_task.customer_id) == customer_id
        assert str(added_task.campaign_id) == campaign_id
        assert added_task.task_type == "llm_analysis"
        assert added_task.priority == TaskPriority.HOT
        assert added_task.status == TaskStatus.PENDING
        assert added_task.payload == {"test": "data"}
        assert added_task.max_attempts == 3


@pytest.mark.asyncio
async def test_claim_task_success():
    """Test that claim_task returns True when the update executes successfully."""
    with patch("src.services.task_manager.async_session_factory") as mock_session_factory:
        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        session_instance.commit = AsyncMock()
        
        # Mock execute returning a rowcount > 0
        mock_result = MagicMock()
        mock_result.rowcount = 1
        session_instance.execute = AsyncMock(return_value=mock_result)

        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        task_id = str(uuid4())
        claimed = await task_manager.claim_task(task_id)

        assert claimed is True
        session_instance.execute.assert_called_once()
        session_instance.commit.assert_called_once()


@pytest.mark.asyncio
async def test_claim_task_already_claimed():
    """Test that claim_task returns False when the update rowcount is 0."""
    with patch("src.services.task_manager.async_session_factory") as mock_session_factory:
        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        session_instance.commit = AsyncMock()
        
        mock_result = MagicMock()
        mock_result.rowcount = 0
        session_instance.execute = AsyncMock(return_value=mock_result)

        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        task_id = str(uuid4())
        claimed = await task_manager.claim_task(task_id)

        assert claimed is False
        session_instance.execute.assert_called_once()
        session_instance.commit.assert_called_once()


@pytest.mark.asyncio
async def test_complete_task():
    """Test that complete_task commits the database update with completed status."""
    with patch("src.services.task_manager.async_session_factory") as mock_session_factory:
        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        session_instance.commit = AsyncMock()
        session_instance.execute = AsyncMock()
        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        task_id = str(uuid4())
        await task_manager.complete_task(task_id, result={"outcome": "success"})

        session_instance.execute.assert_called_once()
        session_instance.commit.assert_called_once()


@pytest.mark.asyncio
async def test_fail_task_retry():
    """Test that fail_task schedules a retry if under max_attempts."""
    with patch("src.services.task_manager.async_session_factory") as mock_session_factory:
        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        session_instance.commit = AsyncMock()
        
        # Mock query return: attempt_count=0, max_attempts=3
        mock_row = MagicMock()
        mock_row.attempt_count = 0
        mock_row.max_attempts = 3
        
        mock_result = MagicMock()
        mock_result.first = MagicMock(return_value=mock_row)
        session_instance.execute = AsyncMock(side_effect=[mock_result, MagicMock()])

        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        task_id = str(uuid4())
        await task_manager.fail_task(task_id, error_message="LLM timeout", backoff_seconds=10)

        # Assert query and then update were run
        assert session_instance.execute.call_count == 2
        session_instance.commit.assert_called_once()


@pytest.mark.asyncio
async def test_fail_task_dead_letter():
    """Test that fail_task dead letters when attempt_count matches max_attempts."""
    with patch("src.services.task_manager.async_session_factory") as mock_session_factory:
        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        session_instance.commit = AsyncMock()
        
        # Mock query return: attempt_count=2, max_attempts=3 (next attempt will be 3 -> dead letter)
        mock_row = MagicMock()
        mock_row.attempt_count = 2
        mock_row.max_attempts = 3
        
        mock_result = MagicMock()
        mock_result.first = MagicMock(return_value=mock_row)
        session_instance.execute = AsyncMock(side_effect=[mock_result, MagicMock()])

        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        task_id = str(uuid4())
        
        with patch("src.services.task_manager.logger") as mock_logger:
            await task_manager.fail_task(task_id, error_message="LLM 429 Limit Exceeded", backoff_seconds=10)

            # Check that error log was written
            mock_logger.error.assert_called_once()
            assert "ALERT: Dead letter task created" in mock_logger.error.call_args[0][0]

        assert session_instance.execute.call_count == 2
        session_instance.commit.assert_called_once()


@pytest.mark.asyncio
async def test_reset_stuck_tasks():
    """Test that reset_stuck_tasks commits the reclamation update."""
    with patch("src.services.task_manager.async_session_factory") as mock_session_factory:
        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        session_instance.commit = AsyncMock()
        
        mock_result = MagicMock()
        mock_result.rowcount = 4
        session_instance.execute = AsyncMock(return_value=mock_result)

        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        count = await task_manager.reset_stuck_tasks(timeout_seconds=300)

        assert count == 4
        session_instance.execute.assert_called_once()
        session_instance.commit.assert_called_once()
