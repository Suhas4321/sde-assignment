import uuid
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from src.models.base import Base


class AuditLog(Base):
    __tablename__ = "audit_log"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    interaction_id = Column(
        UUID(as_uuid=True), ForeignKey("interactions.id", ondelete="SET NULL"), nullable=True, index=True
    )
    customer_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    campaign_id = Column(UUID(as_uuid=True), nullable=False)
    event_type = Column(String(100), nullable=False)
    step = Column(String(100), nullable=False)
    status = Column(String(50), nullable=False)  # "started", "completed", "failed"
    attempt = Column(Integer, default=1, nullable=False)
    event_metadata = Column(JSONB, default=dict, nullable=False)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
