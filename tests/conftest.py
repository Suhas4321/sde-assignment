import json
import os
import pytest
import pytest_asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Create mock redis early and patch the module before other modules import it
mock_redis_instance = AsyncMock()
mock_redis_instance.get = AsyncMock(return_value=None)
mock_redis_instance.set = AsyncMock()
mock_redis_instance.incr = AsyncMock(return_value=1)
mock_redis_instance.decr = AsyncMock(return_value=0)
mock_redis_instance.expire = AsyncMock()
mock_redis_instance.rpush = AsyncMock()
mock_redis_instance.lpop = AsyncMock(return_value=None)
mock_redis_instance.llen = AsyncMock(return_value=0)
mock_redis_instance.hset = AsyncMock()
mock_redis_instance.hget = AsyncMock(return_value=None)
mock_redis_instance.pipeline = MagicMock()

import src.utils.redis_client
src.utils.redis_client.redis_client = mock_redis_instance

from src.services.post_call_processor import PostCallContext
from datetime import datetime


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_transcripts():
    with open(FIXTURES_DIR / "sample_transcripts.json") as f:
        return json.load(f)["transcripts"]


@pytest.fixture
def make_post_call_context(sample_transcripts):
    def _factory(transcript_key: str = "rebook_confirmed", **overrides):
        transcript_data = sample_transcripts[transcript_key]
        transcript = transcript_data["transcript"]
        transcript_text = "\n".join(
            f"{turn['role']}: {turn['content']}" for turn in transcript
        )

        defaults = {
            "interaction_id": "test-interaction-001",
            "session_id": "test-session-001",
            "lead_id": "test-lead-001",
            "campaign_id": "test-campaign-001",
            "customer_id": "test-customer-001",
            "agent_id": "test-agent-001",
            "call_sid": "test-call-sid-001",
            "transcript_text": transcript_text,
            "conversation_data": {"transcript": transcript},
            "additional_data": {},
            "ended_at": datetime.utcnow(),
            "exotel_account_id": "test-exotel-account",
        }
        defaults.update(overrides)
        return PostCallContext(**defaults)

    return _factory


@pytest.fixture
def mock_redis():
    mock_redis_instance.reset_mock()
    mock_redis_instance.get.return_value = None
    mock_redis_instance.set.return_value = None
    mock_redis_instance.incr.return_value = 1
    mock_redis_instance.decr.return_value = 0
    mock_redis_instance.expire.return_value = None
    mock_redis_instance.rpush.return_value = None
    mock_redis_instance.lpop.return_value = None
    mock_redis_instance.llen.return_value = 0
    mock_redis_instance.hset.return_value = None
    mock_redis_instance.hget.return_value = None
    return mock_redis_instance


