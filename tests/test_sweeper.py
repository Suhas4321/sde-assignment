import pytest
from unittest.mock import AsyncMock, patch
from src.tasks.celery_tasks import reset_stuck_tasks_periodic


def test_reset_stuck_tasks_periodic_success():
    """Verify that reset_stuck_tasks_periodic executes successfully, calls reset_stuck_tasks, and logs reclaimed count."""
    with patch("src.tasks.celery_tasks.task_manager.reset_stuck_tasks", new_callable=AsyncMock) as mock_reset, \
         patch("src.tasks.celery_tasks.logger") as mock_logger:
        
        mock_reset.return_value = 5

        reset_stuck_tasks_periodic()

        mock_reset.assert_called_once_with(timeout_seconds=600)
        mock_logger.info.assert_called_once_with("reclaimed_stuck_tasks", extra={"count": 5})


def test_reset_stuck_tasks_periodic_no_stuck():
    """Verify that reset_stuck_tasks_periodic does not log if there are no stuck tasks."""
    with patch("src.tasks.celery_tasks.task_manager.reset_stuck_tasks", new_callable=AsyncMock) as mock_reset, \
         patch("src.tasks.celery_tasks.logger") as mock_logger:
        
        mock_reset.return_value = 0

        reset_stuck_tasks_periodic()

        mock_reset.assert_called_once_with(timeout_seconds=600)
        mock_logger.info.assert_not_called()


def test_reset_stuck_tasks_periodic_failure():
    """Verify that exceptions raised in reset_stuck_tasks are caught and logged as an exception."""
    with patch("src.tasks.celery_tasks.task_manager.reset_stuck_tasks", new_callable=AsyncMock) as mock_reset, \
         patch("src.tasks.celery_tasks.logger") as mock_logger:
        
        mock_reset.side_effect = RuntimeError("DB connection lost")

        reset_stuck_tasks_periodic()

        mock_reset.assert_called_once_with(timeout_seconds=600)
        mock_logger.exception.assert_called_once_with(
            "reset_stuck_tasks_periodic_failed",
            extra={"error": "DB connection lost"}
        )
