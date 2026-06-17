import logging
import time
from datetime import datetime, timedelta
from sqlalchemy import select, func
from src.utils.redis_client import redis_client
from src.models.processing_task import ProcessingTask, TaskStatus

logger = logging.getLogger(__name__)


async def trigger_alert_checks(session) -> None:
    """
    Run periodic checks for queue backlog and recording failure rate.
    Throttled via Redis to avoid overloading the database.
    """
    await check_backlog_alert(session)
    await check_recording_failure_alert(session)


async def check_backlog_alert(session) -> None:
    try:
        # Throttle check to once every 10 seconds
        checked = await redis_client.get("alert:backlog:last_checked")
        if checked:
            return

        await redis_client.set("alert:backlog:last_checked", "1", ex=10)

        # Count pending tasks
        stmt = select(func.count(ProcessingTask.id)).where(ProcessingTask.status == TaskStatus.PENDING)
        res = await session.execute(stmt)
        pending_count = res.scalar() or 0

        if pending_count > 10000:
            logger.error(
                "ALERT: Processing backlog threshold exceeded",
                extra={"pending_count": pending_count}
            )
    except Exception as e:
        logger.warning(f"Failed to check backlog alert: {e}")


async def check_recording_failure_alert(session) -> None:
    try:
        # Throttle check to once every 60 seconds
        checked = await redis_client.get("alert:rec_fail:last_checked")
        if checked:
            return

        await redis_client.set("alert:rec_fail:last_checked", "1", ex=60)

        cutoff = datetime.utcnow() - timedelta(minutes=15)
        
        # Query total recording tasks in the last 15 minutes
        stmt_total = select(func.count(ProcessingTask.id)).where(
            ProcessingTask.task_type == "recording_upload",
            ProcessingTask.updated_at >= cutoff
        )
        res_total = await session.execute(stmt_total)
        total_count = res_total.scalar() or 0

        if total_count == 0:
            return

        # Query failed recording tasks in the last 15 minutes
        stmt_failed = select(func.count(ProcessingTask.id)).where(
            ProcessingTask.task_type == "recording_upload",
            ProcessingTask.updated_at >= cutoff,
            ProcessingTask.status.in_([TaskStatus.FAILED, TaskStatus.DEAD_LETTER])
        )
        res_failed = await session.execute(stmt_failed)
        failed_count = res_failed.scalar() or 0

        failure_rate = failed_count / total_count
        if total_count >= 10 and failure_rate > 0.10:
            logger.error(
                "ALERT: Recording failure rate high",
                extra={
                    "failed_count": failed_count,
                    "total_count": total_count,
                    "failure_rate": round(failure_rate, 4),
                }
            )
    except Exception as e:
        logger.warning(f"Failed to check recording failure alert: {e}")
