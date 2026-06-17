"""
Celery tasks for decoupled post-call processing pipeline.
Separates recording upload and LLM analysis tasks.
"""

import asyncio
import logging
from datetime import datetime
from uuid import UUID

from src.tasks.celery_app import celery_app
from src.services.post_call_processor import PostCallProcessor, PostCallContext
from src.services.recording import fetch_and_upload_recording
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage
from src.services.metrics import metrics_tracker
from src.services.task_manager import task_manager
from src.services.audit_logger import log_event
from src.services.token_budget import token_budget_manager
from src.services.rate_limiter import global_rate_limiter
from src.utils.db import async_session_factory
from src.models.processing_task import ProcessingTask, TaskStatus, TaskPriority
from src.models.interaction import Interaction
from src.config import settings

from sqlalchemy import select, update

logger = logging.getLogger(__name__)


@celery_app.task(
    name="process_recording_upload_task",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    acks_late=True,
    queue="postcall_processing",
)
def process_recording_upload_task(self, task_id: str):
    """
    Celery task dedicated to polling and uploading call recordings.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_process_recording_upload(self, task_id))
    except Exception as e:
        logger.exception("recording_task_failed", extra={"task_id": task_id, "error": str(e)})
        loop.run_until_complete(task_manager.fail_task(task_id, str(e)))
        raise self.retry(exc=e)
    finally:
        loop.close()


async def _process_recording_upload(task, task_id: str):
    # 1. Claim task
    claimed = await task_manager.claim_task(task_id)
    if not claimed:
        return

    # 2. Get task details
    async with async_session_factory() as session:
        res = await session.execute(select(ProcessingTask).where(ProcessingTask.id == UUID(task_id)))
        pt = res.scalars().first()
        if not pt:
            raise ValueError(f"ProcessingTask {task_id} not found in DB")
        interaction_id = str(pt.interaction_id)
        customer_id = str(pt.customer_id)
        campaign_id = str(pt.campaign_id)
        call_sid = pt.payload.get("call_sid")
        exotel_account_id = pt.payload.get("exotel_account_id")

    # 3. Log event
    await log_event(
        interaction_id=interaction_id,
        event_type="recording_attempt",
        step="recording_upload",
        status="started",
        customer_id=customer_id,
        campaign_id=campaign_id,
        attempt=pt.attempt_count + 1,
    )

    # 4. Update status to 'polling' in interactions
    async with async_session_factory() as session:
        await session.execute(
            update(Interaction)
            .where(Interaction.id == UUID(interaction_id))
            .values(recording_status="polling")
        )
        await session.commit()

    # 5. Call poller
    s3_key = await fetch_and_upload_recording(
        interaction_id=interaction_id,
        call_sid=call_sid,
        exotel_account_id=exotel_account_id or "",
    )

    # 6. Update database based on outcome
    async with async_session_factory() as session:
        if s3_key:
            # Success
            await session.execute(
                update(Interaction)
                .where(Interaction.id == UUID(interaction_id))
                .values(
                    recording_s3_key=s3_key,
                    recording_status="uploaded",
                    recording_attempts=pt.attempt_count + 1
                )
            )
            await session.commit()

            await log_event(
                interaction_id=interaction_id,
                event_type="recording_success",
                step="recording_upload",
                status="completed",
                customer_id=customer_id,
                campaign_id=campaign_id,
                attempt=pt.attempt_count + 1,
                metadata={"s3_key": s3_key}
            )
            await task_manager.complete_task(task_id, {"s3_key": s3_key})
        else:
            # Failure
            await session.execute(
                update(Interaction)
                .where(Interaction.id == UUID(interaction_id))
                .values(
                    recording_status="failed",
                    recording_attempts=pt.attempt_count + 1
                )
            )
            await session.commit()

            await log_event(
                interaction_id=interaction_id,
                event_type="recording_failed",
                step="recording_upload",
                status="failed",
                customer_id=customer_id,
                campaign_id=campaign_id,
                attempt=pt.attempt_count + 1,
            )
            await task_manager.complete_task(task_id, {"status": "failed"})


@celery_app.task(
    name="process_llm_analysis_task",
    bind=True,
    max_retries=10,
    default_retry_delay=5,
    acks_late=True,
    queue="postcall_processing",
)
def process_llm_analysis_task(self, task_id: str):
    """
    Celery task dedicated to performing rate-limited LLM analysis and downstream jobs.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_process_llm_analysis(self, task_id))
    except Exception as e:
        logger.exception("llm_task_failed", extra={"task_id": task_id, "error": str(e)})
        if "rate_limit_exceeded" in str(e) or "budget_exhausted" in str(e):
            raise self.retry(exc=e, countdown=5)
        loop.run_until_complete(task_manager.fail_task(task_id, str(e)))
        raise self.retry(exc=e)
    finally:
        loop.close()


async def _process_llm_analysis(task, task_id: str):
    # 1. Claim task
    claimed = await task_manager.claim_task(task_id)
    if not claimed:
        return

    # 2. Get task details
    async with async_session_factory() as session:
        res = await session.execute(select(ProcessingTask).where(ProcessingTask.id == UUID(task_id)))
        pt = res.scalars().first()
        if not pt:
            raise ValueError(f"ProcessingTask {task_id} not found in DB")
        interaction_id = str(pt.interaction_id)
        customer_id = str(pt.customer_id)
        campaign_id = str(pt.campaign_id)
        priority = pt.priority
        payload = pt.payload

    await log_event(
        interaction_id=interaction_id,
        event_type="llm_started",
        step="llm_analysis",
        status="started",
        customer_id=customer_id,
        campaign_id=campaign_id,
        attempt=pt.attempt_count + 1,
    )

    # 3. Fast Path for Short Calls (Skip LLM)
    if priority == TaskPriority.SKIP:
        await log_event(
            interaction_id=interaction_id,
            event_type="llm_skipped",
            step="llm_analysis",
            status="completed",
            customer_id=customer_id,
            campaign_id=campaign_id,
            metadata={"reason": "short_transcript"}
        )
        
        # Trigger downstream events durably
        await trigger_signal_jobs(
            interaction_id=interaction_id,
            session_id=payload["session_id"],
            campaign_id=campaign_id,
            analysis_result={"call_stage": "short_call"},
        )
        await update_lead_stage(
            lead_id=payload["lead_id"],
            interaction_id=interaction_id,
            call_stage="short_call",
        )
        
        await task_manager.complete_task(task_id, {"call_stage": "short_call", "skipped": True})
        return

    # 4. Rate Limiter and Token Budget Checks
    estimated_tokens = settings.LLM_AVG_TOKENS_PER_CALL

    # Check Customer Budget
    budget_ok, pool_used = await token_budget_manager.check_and_reserve_budget(customer_id, estimated_tokens)
    if not budget_ok:
        async with async_session_factory() as session:
            await session.execute(
                update(ProcessingTask)
                .where(ProcessingTask.id == UUID(task_id))
                .values(status=TaskStatus.PENDING)
            )
            await session.commit()
        await log_event(
            interaction_id=interaction_id,
            event_type="llm_rate_limited",
            step="llm_analysis",
            status="failed",
            customer_id=customer_id,
            campaign_id=campaign_id,
            metadata={"reason": "customer_budget_exhausted"}
        )
        raise Exception("budget_exhausted")

    # Check Global Rate Limiter
    global_ok = await global_rate_limiter.acquire(estimated_tokens)
    if not global_ok:
        # Refund customer budget first
        await token_budget_manager.release_budget(customer_id, pool_used, estimated_tokens, 0)
        
        async with async_session_factory() as session:
            await session.execute(
                update(ProcessingTask)
                .where(ProcessingTask.id == UUID(task_id))
                .values(status=TaskStatus.PENDING)
            )
            await session.commit()
        await log_event(
            interaction_id=interaction_id,
            event_type="llm_rate_limited",
            step="llm_analysis",
            status="failed",
            customer_id=customer_id,
            campaign_id=campaign_id,
            metadata={"reason": "global_rate_limit_exceeded"}
        )
        raise Exception("rate_limit_exceeded")

    # 5. Perform LLM analysis
    ctx = PostCallContext(
        interaction_id=interaction_id,
        session_id=payload["session_id"],
        lead_id=payload["lead_id"],
        campaign_id=campaign_id,
        customer_id=customer_id,
        agent_id=payload.get("agent_id", "00000000-0000-0000-0000-000000000000"),
        call_sid=payload.get("call_sid", ""),
        transcript_text=payload.get("transcript_text", ""),
        conversation_data=payload.get("conversation_data", {}),
        additional_data=payload.get("additional_data", {}),
        ended_at=datetime.utcnow(),
    )

    processor = PostCallProcessor()
    result = await processor.process_post_call(ctx, single_prompt=True)

    # 6. Release/refund unused token budget
    await token_budget_manager.release_budget(customer_id, pool_used, estimated_tokens, result.tokens_used)
    await global_rate_limiter.release(estimated_tokens, result.tokens_used)

    # Update tokens used on interaction table
    async with async_session_factory() as session:
        await session.execute(
            update(Interaction)
            .where(Interaction.id == UUID(interaction_id))
            .values(
                llm_tokens_used=result.tokens_used,
                processing_completed_at=datetime.utcnow()
            )
        )
        await session.commit()

    # 7. Trigger Downstream Events
    try:
        await trigger_signal_jobs(
            interaction_id=interaction_id,
            session_id=ctx.session_id,
            campaign_id=campaign_id,
            analysis_result=result.raw_response,
        )
    except Exception as e:
        logger.warning("signal_jobs_failed", extra={"error": str(e)})

    try:
        await update_lead_stage(
            lead_id=ctx.lead_id,
            interaction_id=interaction_id,
            call_stage=result.call_stage,
        )
    except Exception as e:
        logger.warning("lead_stage_update_failed", extra={"error": str(e)})

    # Dispatch CRM Webhook Push Task if webhook URL is configured
    try:
        from src.services.customer_config_manager import customer_config_manager
        _, crm_webhook_url = await customer_config_manager.get_config(customer_id)
        if crm_webhook_url:
            crm_task_id = await task_manager.create_task(
                interaction_id=interaction_id,
                customer_id=customer_id,
                campaign_id=campaign_id,
                task_type="crm_push",
                priority="cold",
                payload={
                    "crm_webhook_url": crm_webhook_url,
                    "analysis_result": result.raw_response
                }
            )
            process_crm_push_task.apply_async(
                args=[crm_task_id],
                queue="postcall_processing",
            )
            await log_event(
                interaction_id=interaction_id,
                event_type="crm_push_enqueued",
                step="llm_analysis",
                status="completed",
                customer_id=customer_id,
                campaign_id=campaign_id,
                metadata={"crm_task_id": crm_task_id}
            )
    except Exception as e:
        logger.warning("crm_push_enqueue_failed", extra={"error": str(e)})

    # 8. Complete durable task record
    await task_manager.complete_task(task_id, result.raw_response)
    
    await log_event(
        interaction_id=interaction_id,
        event_type="llm_completed",
        step="llm_analysis",
        status="completed",
        customer_id=customer_id,
        campaign_id=campaign_id,
        metadata={"tokens_used": result.tokens_used, "call_stage": result.call_stage}
    )


@celery_app.task(
    name="process_crm_push_task",
    bind=True,
    max_retries=5,
    default_retry_delay=10,
    acks_late=True,
    queue="postcall_processing",
)
def process_crm_push_task(self, task_id: str):
    """
    Celery task dedicated to performing durable CRM webhook push with retries.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_process_crm_push(self, task_id))
    except Exception as e:
        logger.exception("crm_push_task_failed", extra={"task_id": task_id, "error": str(e)})
        # Durable task failure retry
        loop.run_until_complete(task_manager.fail_task(task_id, str(e), backoff_seconds=15))
        raise self.retry(exc=e)
    finally:
        loop.close()


async def _process_crm_push(task, task_id: str):
    claimed = await task_manager.claim_task(task_id)
    if not claimed:
        return

    # Get task details
    async with async_session_factory() as session:
        res = await session.execute(select(ProcessingTask).where(ProcessingTask.id == UUID(task_id)))
        pt = res.scalars().first()
        if not pt:
            raise ValueError(f"ProcessingTask {task_id} not found in DB")
        interaction_id = str(pt.interaction_id)
        customer_id = str(pt.customer_id)
        campaign_id = str(pt.campaign_id)
        payload = pt.payload

    await log_event(
        interaction_id=interaction_id,
        event_type="crm_push_started",
        step="crm_push",
        status="started",
        customer_id=customer_id,
        campaign_id=campaign_id,
        attempt=pt.attempt_count + 1,
    )

    webhook_url = payload.get("crm_webhook_url")
    if not webhook_url:
        from src.services.customer_config_manager import customer_config_manager
        _, webhook_url = await customer_config_manager.get_config(customer_id)

    if not webhook_url:
        # No webhook URL configured, complete task as no-op
        await task_manager.complete_task(task_id, {"status": "skipped", "reason": "no_webhook_url"})
        await log_event(
            interaction_id=interaction_id,
            event_type="crm_push_skipped",
            step="crm_push",
            status="completed",
            customer_id=customer_id,
            campaign_id=campaign_id,
            metadata={"reason": "no_webhook_url"}
        )
        return

    # Send Webhook POST request
    import httpx
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(webhook_url, json=payload.get("analysis_result", {}))
        response.raise_for_status()

    # Success: Complete task and log success event
    result_payload = {"status": "success", "status_code": response.status_code}
    await task_manager.complete_task(task_id, result_payload)
    await log_event(
        interaction_id=interaction_id,
        event_type="crm_push_success",
        step="crm_push",
        status="completed",
        customer_id=customer_id,
        campaign_id=campaign_id,
        metadata=result_payload
    )


@celery_app.task(name="reset_stuck_tasks_periodic")
def reset_stuck_tasks_periodic():
    """
    Periodic task to find tasks stuck in in_progress for too long and reset them to pending.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        count = loop.run_until_complete(task_manager.reset_stuck_tasks(timeout_seconds=600))
        if count > 0:
            logger.info("reclaimed_stuck_tasks", extra={"count": count})
    except Exception as e:
        logger.exception("reset_stuck_tasks_periodic_failed", extra={"error": str(e)})
    finally:
        loop.close()

