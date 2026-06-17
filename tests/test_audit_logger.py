import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock
from src.services.audit_logger import log_event


@pytest.mark.asyncio
async def test_audit_log_fallback_on_db_error():
    """Test that audit logger logs to console and handles database session errors gracefully."""
    with patch("src.services.audit_logger.logger") as mock_logger, \
         patch("src.services.audit_logger.async_session_factory") as mock_session_factory:

        # Simulate DB connection failure
        mock_session = AsyncMock()
        mock_session.__aenter__.side_effect = Exception("DB Connection Refused")
        mock_session_factory.return_value = mock_session

        # Call the log_event function
        await log_event(
            interaction_id="00000000-0000-0000-0000-000000000001",
            event_type="recording_success",
            step="recording_upload",
            status="completed",
            customer_id="00000000-0000-0000-0000-000000000002",
            campaign_id="00000000-0000-0000-0000-000000000003",
            attempt=1,
            metadata={"s3_key": "recordings/test.mp3"}
        )

        # Verify console log occurred
        mock_logger.info.assert_called_once()
        log_str = mock_logger.info.call_args[0][0]
        log_json = json.loads(log_str)

        assert log_json["event_type"] == "recording_success"
        assert log_json["status"] == "completed"
        assert log_json["metadata"]["s3_key"] == "recordings/test.mp3"

        # Verify DB warning log occurred due to side_effect Exception
        mock_logger.warning.assert_called_once()
        warning_str = mock_logger.warning.call_args[0][0]
        assert "Failed to persist audit log to DB" in warning_str


@pytest.mark.asyncio
async def test_audit_log_db_success():
    """Test that audit logger writes log record to the database successfully."""
    with patch("src.services.audit_logger.logger") as mock_logger, \
         patch("src.services.audit_logger.async_session_factory") as mock_session_factory:

        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        session_instance.commit = AsyncMock()
        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        await log_event(
            interaction_id="00000000-0000-0000-0000-000000000001",
            event_type="llm_started",
            step="llm_analysis",
            status="started",
            customer_id="00000000-0000-0000-0000-000000000002",
            campaign_id="00000000-0000-0000-0000-000000000003",
        )

        # Verify session commit was called
        session_instance.add.assert_called_once()
        session_instance.commit.assert_called_once()
        mock_logger.info.assert_called_once()
