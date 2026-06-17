import pytest
from unittest.mock import AsyncMock, patch
import httpx
from src.services.recording import fetch_and_upload_recording

@pytest.mark.asyncio
async def test_recording_success_first_try():
    """Test that recording is successfully fetched and uploaded on the first try."""
    with patch("src.services.recording._fetch_exotel_recording_url", new_callable=AsyncMock) as mock_fetch, \
         patch("src.services.recording._upload_to_s3", new_callable=AsyncMock) as mock_upload, \
         patch("src.services.recording.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:

        mock_fetch.return_value = "https://exotel.com/recording.mp3"
        mock_upload.return_value = "recordings/test-interaction-1.mp3"

        res = await fetch_and_upload_recording(
            interaction_id="test-interaction-1",
            call_sid="test-call-1",
            exotel_account_id="test-acc-1"
        )

        assert res == "recordings/test-interaction-1.mp3"
        mock_fetch.assert_called_once_with("test-call-1", "test-acc-1")
        mock_upload.assert_called_once_with("https://exotel.com/recording.mp3", "test-interaction-1")
        mock_sleep.assert_not_called()

@pytest.mark.asyncio
async def test_recording_success_after_retries():
    """Test that recording is retried and succeeds after a few 404s."""
    with patch("src.services.recording._fetch_exotel_recording_url", new_callable=AsyncMock) as mock_fetch, \
         patch("src.services.recording._upload_to_s3", new_callable=AsyncMock) as mock_upload, \
         patch("src.services.recording.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:

        # Returns None (404) twice, then a URL
        mock_fetch.side_effect = [None, None, "https://exotel.com/recording.mp3"]
        mock_upload.return_value = "recordings/test-interaction-2.mp3"

        res = await fetch_and_upload_recording(
            interaction_id="test-interaction-2",
            call_sid="test-call-2",
            exotel_account_id="test-acc-2"
        )

        assert res == "recordings/test-interaction-2.mp3"
        assert mock_fetch.call_count == 3
        assert mock_sleep.call_count == 2
        # Verify it slept with [5, 10] backoff schedule
        mock_sleep.assert_any_call(5)
        mock_sleep.assert_any_call(10)

@pytest.mark.asyncio
async def test_recording_permanent_failure():
    """Test that recording is retried 7 times and returns None if never ready."""
    with patch("src.services.recording._fetch_exotel_recording_url", new_callable=AsyncMock) as mock_fetch, \
         patch("src.services.recording._upload_to_s3", new_callable=AsyncMock) as mock_upload, \
         patch("src.services.recording.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:

        mock_fetch.return_value = None

        res = await fetch_and_upload_recording(
            interaction_id="test-interaction-3",
            call_sid="test-call-3",
            exotel_account_id="test-acc-3"
        )

        assert res is None
        assert mock_fetch.call_count == 7  # Initial (1) + 6 retries
        assert mock_sleep.call_count == 6
        mock_upload.assert_not_called()

@pytest.mark.asyncio
async def test_recording_retry_on_network_error():
    """Test that recording retries on network/HTTP exceptions and succeeds."""
    with patch("src.services.recording._fetch_exotel_recording_url", new_callable=AsyncMock) as mock_fetch, \
         patch("src.services.recording._upload_to_s3", new_callable=AsyncMock) as mock_upload, \
         patch("src.services.recording.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:

        # Raises HTTPError, then returns URL
        mock_fetch.side_effect = [httpx.ConnectError("Connection failed"), "https://exotel.com/recording.mp3"]
        mock_upload.return_value = "recordings/test-interaction-4.mp3"

        res = await fetch_and_upload_recording(
            interaction_id="test-interaction-4",
            call_sid="test-call-4",
            exotel_account_id="test-acc-4"
        )

        assert res == "recordings/test-interaction-4.mp3"
        assert mock_fetch.call_count == 2
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_once_with(5)
