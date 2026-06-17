import logging
import json
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy import select
from src.utils.db import async_session_factory
from src.models.audit_log import AuditLog
from src.models.interaction import Interaction

logger = logging.getLogger("audit")


async def log_event(
    interaction_id: str,
    event_type: str,
    step: str,
    status: str,
    customer_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
    attempt: int = 1,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Log an event to stdout as structured JSON and persist it to Postgres audit_log table.
    Resilient to database connection failures (falls back to console-only logging).
    """
    now = datetime.utcnow()
    metadata = metadata or {}

    # 1. Console structured logging
    log_payload = {
        "timestamp": now.isoformat(),
        "event_type": event_type,
        "step": step,
        "status": status,
        "interaction_id": interaction_id,
        "customer_id": customer_id,
        "campaign_id": campaign_id,
        "attempt": attempt,
        "metadata": metadata,
    }
    
    # Log to stdout at appropriate level
    if status == "failed":
        logger.error(json.dumps(log_payload))
    else:
        logger.info(json.dumps(log_payload))

    # 2. Persist to Postgres database
    try:
        async with async_session_factory() as session:
            # Resolve customer_id and campaign_id if not provided
            if not customer_id or not campaign_id:
                try:
                    stmt = select(Interaction.customer_id, Interaction.campaign_id).where(
                        Interaction.id == UUID(interaction_id)
                    )
                    res = await session.execute(stmt)
                    row = res.first()
                    if row:
                        customer_id = customer_id or str(row.customer_id)
                        campaign_id = campaign_id or str(row.campaign_id)
                except Exception:
                    # Ignore resolution errors (e.g. invalid UUID format or missing row)
                    pass

            # Create audit log record
            db_log = AuditLog(
                interaction_id=UUID(interaction_id) if interaction_id else None,
                customer_id=UUID(customer_id) if customer_id else UUID("00000000-0000-0000-0000-000000000000"),
                campaign_id=UUID(campaign_id) if campaign_id else UUID("00000000-0000-0000-0000-000000000000"),
                event_type=event_type,
                step=step,
                status=status,
                attempt=attempt,
                event_metadata=metadata,
                created_at=now,
            )
            session.add(db_log)
            await session.commit()

    except Exception as e:
        # Fallback: logging to DB failed but we must not crash the caller
        logger.warning(
            f"Failed to persist audit log to DB: {e}",
            extra={"interaction_id": interaction_id, "event_type": event_type},
        )
