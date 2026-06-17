"""
FastAPI endpoint for ending an interaction.

POST /session/{session_id}/interaction/{interaction_id}/end

Called by Exotel (telephony provider) when a call disconnects.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, update

from src.utils.db import async_session_factory
from src.models.interaction import Interaction
from src.services.triage import classify_priority
from src.services.task_manager import task_manager
from src.services.audit_logger import log_event
from src.tasks.celery_tasks import (
    process_recording_upload_task,
    process_llm_analysis_task,
)

logger = logging.getLogger(__name__)
router = APIRouter()


class InteractionEndRequest(BaseModel):
    call_sid: Optional[str] = None
    duration_seconds: Optional[int] = None
    call_status: Optional[str] = None
    additional_data: Optional[Dict[str, Any]] = None


class InteractionEndResponse(BaseModel):
    status: str
    interaction_id: str
    message: str


@router.post(
    "/session/{session_id}/interaction/{interaction_id}/end",
    response_model=InteractionEndResponse,
)
async def end_interaction(
    session_id: UUID,
    interaction_id: UUID,
    request: InteractionEndRequest,
):
    """
    End an interaction and trigger decoupled background post-call tasks.
    """
    try:
        # 1. Log event: postcall_received
        await log_event(
            interaction_id=str(interaction_id),
            event_type="postcall_received",
            step="endpoint_webhook",
            status="started",
        )

        interaction = await _load_interaction(interaction_id)

        if not interaction:
            await log_event(
                interaction_id=str(interaction_id),
                event_type="postcall_received",
                step="endpoint_webhook",
                status="failed",
                metadata={"error": "Interaction not found"}
            )
            raise HTTPException(status_code=404, detail="Interaction not found")

        # 2. Update status in database
        await _update_interaction_status(
            interaction_id=str(interaction_id),
            status="ENDED",
            ended_at=datetime.utcnow(),
            duration=request.duration_seconds,
            call_sid=request.call_sid,
        )

        # 3. Classify Priority
        transcript = interaction.conversation_data.get("transcript", []) if interaction.conversation_data else []
        is_short = len(transcript) < 4
        
        # Get custom customer configurations
        from src.services.customer_config_manager import customer_config_manager
        hot_keywords, crm_webhook_url = await customer_config_manager.get_config(str(interaction.customer_id))

        # Get raw transcript text
        transcript_text = interaction.transcript_text
        priority = "skip" if is_short else classify_priority(transcript_text, custom_keywords=hot_keywords)

        # 4. Save updates to interaction table in DB
        async with async_session_factory() as session:
            stmt = (
                update(Interaction)
                .where(Interaction.id == interaction_id)
                .values(
                    processing_priority=priority,
                    recording_status="pending",
                    processing_started_at=datetime.utcnow(),
                )
            )
            await session.execute(stmt)
            await session.commit()

        # 5. Create Durable postgres tasks and dispatch to Celery
        # Task A: Recording Upload
        recording_task_id = await task_manager.create_task(
            interaction_id=str(interaction_id),
            customer_id=str(interaction.customer_id),
            campaign_id=str(interaction.campaign_id),
            task_type="recording_upload",
            priority="cold",
            payload={
                "call_sid": request.call_sid,
                "exotel_account_id": interaction.exotel_account_id,
            }
        )
        
        celery_recording_task = process_recording_upload_task.apply_async(
            args=[recording_task_id],
            queue="postcall_processing",
        )

        await log_event(
            interaction_id=str(interaction_id),
            event_type="recording_enqueued",
            step="endpoint_webhook",
            status="completed",
            customer_id=str(interaction.customer_id),
            campaign_id=str(interaction.campaign_id),
            metadata={"celery_task_id": celery_recording_task.id}
        )

        # Task B: LLM Analysis / Fast path
        llm_task_id = await task_manager.create_task(
            interaction_id=str(interaction_id),
            customer_id=str(interaction.customer_id),
            campaign_id=str(interaction.campaign_id),
            task_type="llm_analysis",
            priority=priority,
            payload={
                "session_id": str(session_id),
                "lead_id": str(interaction.lead_id),
                "transcript_text": transcript_text,
                "conversation_data": interaction.conversation_data,
                "additional_data": request.additional_data or {},
            }
        )

        # Dispatch LLM task with correct priority countdown / queue
        celery_llm_task = process_llm_analysis_task.apply_async(
            args=[llm_task_id],
            queue="postcall_processing",
            priority=10 if priority == "hot" else 1,
        )

        await log_event(
            interaction_id=str(interaction_id),
            event_type="llm_enqueued",
            step="endpoint_webhook",
            status="completed",
            customer_id=str(interaction.customer_id),
            campaign_id=str(interaction.campaign_id),
            metadata={
                "celery_task_id": celery_llm_task.id,
                "priority": priority,
            }
        )

        return InteractionEndResponse(
            status="ok",
            interaction_id=str(interaction_id),
            message="Interaction ended, processing enqueued",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "end_interaction_failed",
            extra={"interaction_id": str(interaction_id), "error": str(e)},
        )
        raise HTTPException(status_code=500, detail="Internal server error")


async def _load_interaction(interaction_id: UUID) -> Optional[Interaction]:
    """
    Load interaction from the database.
    """
    try:
        async with async_session_factory() as session:
            stmt = select(Interaction).where(Interaction.id == interaction_id)
            res = await session.execute(stmt)
            return res.scalars().first()
    except Exception as e:
        logger.error(f"Failed to load interaction from DB: {e}")
        return None


async def _update_interaction_status(
    interaction_id: str,
    status: str,
    ended_at: datetime,
    duration: Optional[int],
    call_sid: Optional[str],
) -> None:
    """
    Update interaction status in the database.
    """
    try:
        async with async_session_factory() as session:
            stmt = (
                update(Interaction)
                .where(Interaction.id == UUID(interaction_id))
                .values(
                    status=status,
                    ended_at=ended_at,
                    duration_seconds=duration,
                    call_sid=call_sid,
                    updated_at=datetime.utcnow()
                )
            )
            await session.execute(stmt)
            await session.commit()
    except Exception as e:
        logger.error(f"Failed to update interaction status in DB: {e}")
