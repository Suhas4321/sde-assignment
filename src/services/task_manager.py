import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy import select, update
from src.utils.db import async_session_factory
from src.models.processing_task import ProcessingTask, TaskStatus, TaskPriority
from src.services.alerting import trigger_alert_checks

logger = logging.getLogger(__name__)


class TaskManager:
    """
    Service responsible for managing Postgres-backed task state transitions durably.
    Provides the Repository pattern layer for task execution lifecycle.
    """

    async def create_task(
        self,
        interaction_id: str,
        customer_id: str,
        campaign_id: str,
        task_type: str,
        priority: str = "cold",
        payload: Optional[Dict[str, Any]] = None,
        max_attempts: int = 5,
    ) -> str:
        """
        Create a new background task in the database with PENDING status.
        """
        payload = payload or {}
        now = datetime.utcnow()

        async with async_session_factory() as session:
            task = ProcessingTask(
                interaction_id=UUID(interaction_id),
                customer_id=UUID(customer_id),
                campaign_id=UUID(campaign_id),
                task_type=task_type,
                priority=TaskPriority(priority.lower()),
                status=TaskStatus.PENDING,
                attempt_count=0,
                max_attempts=max_attempts,
                payload=payload,
                result={},
                created_at=now,
                updated_at=now,
            )
            session.add(task)
            await trigger_alert_checks(session)
            await session.commit()
            return str(task.id)

    async def claim_task(self, task_id: str) -> bool:
        """
        Atomically transition task from PENDING/FAILED to IN_PROGRESS.
        Returns True if claimed successfully, False if already claimed or invalid.
        """
        now = datetime.utcnow()
        async with async_session_factory() as session:
            # Atomic update to prevent multiple workers from claiming the same task
            stmt = (
                update(ProcessingTask)
                .where(
                    ProcessingTask.id == UUID(task_id),
                    ProcessingTask.status.in_([TaskStatus.PENDING, TaskStatus.FAILED])
                )
                .values(
                    status=TaskStatus.IN_PROGRESS,
                    started_at=now,
                    updated_at=now
                )
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0

    async def complete_task(self, task_id: str, result: Optional[Dict[str, Any]] = None) -> None:
        """
        Transition task to COMPLETED and record final result payload.
        """
        result = result or {}
        now = datetime.utcnow()
        async with async_session_factory() as session:
            stmt = (
                update(ProcessingTask)
                .where(ProcessingTask.id == UUID(task_id))
                .values(
                    status=TaskStatus.COMPLETED,
                    result=result,
                    completed_at=now,
                    updated_at=now
                )
            )
            await session.execute(stmt)
            await session.commit()

    async def fail_task(self, task_id: str, error_message: str, backoff_seconds: int = 60) -> None:
        """
        Handle task failure: increments attempt count and schedules a retry or marks as DEAD_LETTER.
        """
        now = datetime.utcnow()
        async with async_session_factory() as session:
            # Fetch the current attempt metrics
            stmt = select(ProcessingTask.attempt_count, ProcessingTask.max_attempts).where(
                ProcessingTask.id == UUID(task_id)
            )
            res = await session.execute(stmt)
            row = res.first()
            if not row:
                return

            new_attempt = row.attempt_count + 1
            max_attempts = row.max_attempts

            if new_attempt >= max_attempts:
                # Mark as dead letter (permanent failure)
                stmt_update = (
                    update(ProcessingTask)
                    .where(ProcessingTask.id == UUID(task_id))
                    .values(
                        status=TaskStatus.DEAD_LETTER,
                        attempt_count=new_attempt,
                        error_message=error_message,
                        next_retry_at=None,
                        updated_at=now
                    )
                )
                logger.error(
                    "ALERT: Dead letter task created",
                    extra={"task_id": task_id, "attempts": new_attempt, "error": error_message}
                )
            else:
                # Schedule retry using exponential/fixed backoff
                next_retry = now + timedelta(seconds=backoff_seconds)
                stmt_update = (
                    update(ProcessingTask)
                    .where(ProcessingTask.id == UUID(task_id))
                    .values(
                        status=TaskStatus.FAILED,
                        attempt_count=new_attempt,
                        error_message=error_message,
                        next_retry_at=next_retry,
                        updated_at=now
                    )
                )
                logger.warning(
                    "task_attempt_failed",
                    extra={"task_id": task_id, "next_retry": next_retry.isoformat(), "error": error_message}
                )

            await session.execute(stmt_update)
            await trigger_alert_checks(session)
            await session.commit()

    async def reset_stuck_tasks(self, timeout_seconds: int = 600) -> int:
        """
        Reclaim and reset tasks stuck in IN_PROGRESS (e.g. worker crashed).
        Transitions them back to PENDING and increments attempt count.
        """
        now = datetime.utcnow()
        cutoff = now - timedelta(seconds=timeout_seconds)
        async with async_session_factory() as session:
            stmt = (
                update(ProcessingTask)
                .where(
                    ProcessingTask.status == TaskStatus.IN_PROGRESS,
                    ProcessingTask.started_at < cutoff
                )
                .values(
                    status=TaskStatus.PENDING,
                    attempt_count=ProcessingTask.attempt_count + 1,
                    error_message="Worker timeout / process crash detected.",
                    updated_at=now
                )
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount


task_manager = TaskManager()
