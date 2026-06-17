import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from src.services.alerting import check_backlog_alert, check_recording_failure_alert


@pytest.mark.asyncio
async def test_check_backlog_alert_triggers_log():
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar.return_value = 15000
    mock_session.execute.return_value = mock_result

    with patch("src.services.alerting.redis_client") as mock_redis, \
         patch("src.services.alerting.logger") as mock_logger:
        
        mock_redis.get = AsyncMock(return_value=None)
        mock_redis.set = AsyncMock()

        await check_backlog_alert(mock_session)

        mock_redis.set.assert_called_once_with("alert:backlog:last_checked", "1", ex=10)
        mock_logger.error.assert_called_once_with(
            "ALERT: Processing backlog threshold exceeded",
            extra={"pending_count": 15000}
        )


@pytest.mark.asyncio
async def test_check_backlog_alert_throttled():
    mock_session = AsyncMock()

    with patch("src.services.alerting.redis_client") as mock_redis, \
         patch("src.services.alerting.logger") as mock_logger:
        
        mock_redis.get = AsyncMock(return_value="1")

        await check_backlog_alert(mock_session)

        mock_session.execute.assert_not_called()
        mock_logger.error.assert_not_called()


@pytest.mark.asyncio
async def test_recording_failure_alert_triggers_log():
    mock_session = AsyncMock()
    
    mock_total_res = MagicMock()
    mock_total_res.scalar.return_value = 20
    
    mock_failed_res = MagicMock()
    mock_failed_res.scalar.return_value = 5
    
    mock_session.execute.side_effect = [mock_total_res, mock_failed_res]

    with patch("src.services.alerting.redis_client") as mock_redis, \
         patch("src.services.alerting.logger") as mock_logger:
        
        mock_redis.get = AsyncMock(return_value=None)
        mock_redis.set = AsyncMock()

        await check_recording_failure_alert(mock_session)

        mock_redis.set.assert_called_once_with("alert:rec_fail:last_checked", "1", ex=60)
        mock_logger.error.assert_called_once()
        args, kwargs = mock_logger.error.call_args
        assert "ALERT: Recording failure rate high" in args[0]
        assert kwargs["extra"]["failed_count"] == 5
        assert kwargs["extra"]["total_count"] == 20
        assert kwargs["extra"]["failure_rate"] == 0.25
