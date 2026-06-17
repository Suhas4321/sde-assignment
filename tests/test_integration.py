import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from uuid import uuid4
from fastapi.testclient import TestClient

from src.app import app
from src.services.task_manager import task_manager
from src.tasks.celery_tasks import process_llm_analysis_task, process_recording_upload_task
from src.models.processing_task import TaskPriority


@pytest.fixture
def api_client():
    return TestClient(app)


@pytest.mark.asyncio
async def test_endpoint_triggers_both_decoupled_tasks(api_client):
    """Test that the end webhook triages call, creates durable Postgres tasks, and enqueues Celery jobs."""
    session_id = uuid4()
    interaction_id = uuid4()
    mock_interaction = MagicMock()
    mock_interaction.id = str(interaction_id)
    mock_interaction.lead_id = str(uuid4())
    mock_interaction.campaign_id = str(uuid4())
    mock_interaction.customer_id = str(uuid4())
    mock_interaction.agent_id = str(uuid4())
    mock_interaction.conversation_data = {
        "transcript": [
            {"role": "agent", "content": "Hello"},
            {"role": "customer", "content": "I want to confirm appointment rescheduled tomorrow 3 PM"},
            {"role": "agent", "content": "Rescheduled confirmed for tomorrow"},
            {"role": "customer", "content": "Thanks"}
        ]
    }
    mock_interaction.transcript_text = "agent: Hello\ncustomer: I want to confirm appointment rescheduled tomorrow 3 PM\nagent: Rescheduled confirmed for tomorrow\ncustomer: Thanks"
    mock_interaction.exotel_account_id = "test-acc"

    # Patch database calls, Celery dispatches, and audit logger
    with patch("src.api.endpoints._load_interaction", new_callable=AsyncMock) as mock_load, \
         patch("src.api.endpoints._update_interaction_status", new_callable=AsyncMock) as mock_status_update, \
         patch("src.api.endpoints.async_session_factory") as mock_session_factory, \
         patch("src.api.endpoints.task_manager.create_task", new_callable=AsyncMock) as mock_create_task, \
         patch("src.api.endpoints.process_recording_upload_task.apply_async") as mock_celery_rec, \
         patch("src.api.endpoints.process_llm_analysis_task.apply_async") as mock_celery_llm, \
         patch("src.api.endpoints.log_event", new_callable=AsyncMock) as mock_audit:

        mock_load.return_value = mock_interaction
        mock_create_task.side_effect = ["task-recording-id", "task-llm-id"]
        
        # Mock Session Factory context manager
        mock_session_cm = AsyncMock()
        mock_session_cm.__aenter__.return_value = AsyncMock()
        mock_session_factory.return_value = mock_session_cm

        response = api_client.post(
            f"/api/v1/session/{session_id}/interaction/{interaction_id}/end",
            json={
                "call_sid": "exotel-sid-123",
                "duration_seconds": 180,
                "call_status": "completed",
                "additional_data": {"campaign_type": "sales"}
            }
        )

        assert response.status_code == 200
        assert response.json()["status"] == "ok"
        
        # Verify both Celery tasks are enqueued independently
        mock_celery_rec.assert_called_once()
        mock_celery_llm.assert_called_once()
        
        # Verify call priority is triaged as 'hot' due to 'confirm' and 'appointment' keywords
        expected_payload = {
            "session_id": str(session_id),
            "lead_id": str(mock_interaction.lead_id),
            "transcript_text": mock_interaction.transcript_text,
            "conversation_data": mock_interaction.conversation_data,
            "additional_data": {"campaign_type": "sales"},
        }
        mock_create_task.assert_any_call(
            interaction_id=str(interaction_id),
            customer_id=mock_interaction.customer_id,
            campaign_id=mock_interaction.campaign_id,
            task_type="llm_analysis",
            priority="hot",
            payload=expected_payload
        )


@pytest.mark.asyncio
async def test_short_transcripts_skip_llm():
    """Test that a task with SKIP priority skips LLM analysis and goes straight to downstream jobs."""
    task_id = str(uuid4())
    interaction_uuid = str(uuid4())
    customer_uuid = str(uuid4())
    campaign_uuid = str(uuid4())

    mock_pt = MagicMock()
    mock_pt.interaction_id = interaction_uuid
    mock_pt.customer_id = customer_uuid
    mock_pt.campaign_id = campaign_uuid
    mock_pt.priority = TaskPriority.SKIP
    mock_pt.attempt_count = 0
    mock_pt.payload = {
        "session_id": str(uuid4()),
        "lead_id": str(uuid4()),
        "call_sid": "short-call-sid"
    }

    # Patch session factory, task manager, LLM processor, signal jobs, and audit logger
    with patch("src.tasks.celery_tasks.async_session_factory") as mock_session_factory, \
         patch("src.tasks.celery_tasks.task_manager") as mock_tm, \
         patch("src.tasks.celery_tasks.PostCallProcessor") as mock_processor, \
         patch("src.tasks.celery_tasks.trigger_signal_jobs", new_callable=AsyncMock) as mock_signal, \
         patch("src.tasks.celery_tasks.update_lead_stage", new_callable=AsyncMock) as mock_lead, \
         patch("src.tasks.celery_tasks.log_event", new_callable=AsyncMock):

        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        
        mock_scalars = MagicMock()
        mock_scalars.first = MagicMock(return_value=mock_pt)
        
        mock_result = MagicMock()
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        
        session_instance.execute = AsyncMock(return_value=mock_result)
        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        mock_tm.claim_task = AsyncMock(return_value=True)
        mock_tm.complete_task = AsyncMock()

        # Execute task logic (runs inside celery task)
        from src.tasks.celery_tasks import _process_llm_analysis
        await _process_llm_analysis(None, task_id)

        # Assert LLM processor was NEVER called (skipped)
        mock_processor.assert_not_called()

        # Assert downstream jobs were still fired
        mock_signal.assert_called_once_with(
            interaction_id=interaction_uuid,
            session_id=mock_pt.payload["session_id"],
            campaign_id=campaign_uuid,
            analysis_result={"call_stage": "short_call"}
        )
        mock_lead.assert_called_once_with(
            lead_id=mock_pt.payload["lead_id"],
            interaction_id=interaction_uuid,
            call_stage="short_call"
        )
        
        # Verify task completed
        mock_tm.complete_task.assert_called_once_with(task_id, {"call_stage": "short_call", "skipped": True})


@pytest.mark.asyncio
async def test_llm_task_retry_on_rate_limit():
    """Test that LLM task reverts to PENDING and raises Exception when rate limited."""
    task_id = str(uuid4())
    interaction_uuid = str(uuid4())
    customer_uuid = str(uuid4())
    campaign_uuid = str(uuid4())

    mock_pt = MagicMock()
    mock_pt.interaction_id = interaction_uuid
    mock_pt.customer_id = customer_uuid
    mock_pt.campaign_id = campaign_uuid
    mock_pt.priority = TaskPriority.COLD
    mock_pt.attempt_count = 0
    mock_pt.payload = {
        "session_id": str(uuid4()),
        "lead_id": str(uuid4()),
    }

    # Patch components: fail customer budget check
    with patch("src.tasks.celery_tasks.async_session_factory") as mock_session_factory, \
         patch("src.tasks.celery_tasks.task_manager") as mock_tm, \
         patch("src.tasks.celery_tasks.token_budget_manager") as mock_budget, \
         patch("src.tasks.celery_tasks.log_event", new_callable=AsyncMock):

        mock_session_cm = AsyncMock()
        session_instance = MagicMock()
        session_instance.commit = AsyncMock()
        
        mock_scalars = MagicMock()
        mock_scalars.first = MagicMock(return_value=mock_pt)
        
        mock_result = MagicMock()
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        
        session_instance.execute = AsyncMock(return_value=mock_result)
        mock_session_cm.__aenter__.return_value = session_instance
        mock_session_factory.return_value = mock_session_cm

        mock_tm.claim_task = AsyncMock(return_value=True)
        
        # Simulate budget exhaustion
        mock_budget.check_and_reserve_budget = AsyncMock(return_value=(False, "exhausted"))

        from src.tasks.celery_tasks import _process_llm_analysis
        with pytest.raises(Exception, match="budget_exhausted"):
            await _process_llm_analysis(None, task_id)

        # Verify task reverted to PENDING status in DB
        session_instance.execute.assert_called()
