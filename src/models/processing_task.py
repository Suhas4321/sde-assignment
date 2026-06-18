import enum
import uuid
from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from src.models.base import Base
from src.utils.encryption import EncryptedJSONB


class TaskStatus(str, enum.Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


class TaskPriority(str, enum.Enum):
    HOT = "hot"
    COLD = "cold"
    SKIP = "skip"


class ProcessingTask(Base):
    __tablename__ = "processing_tasks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    interaction_id = Column(
        UUID(as_uuid=True), ForeignKey("interactions.id", ondelete="CASCADE"), nullable=False, index=True
    )
    customer_id = Column(UUID(as_uuid=True), nullable=False)
    campaign_id = Column(UUID(as_uuid=True), nullable=False)
    task_type = Column(String(100), nullable=False)  # "llm_analysis", "recording_upload", "signal_jobs", "lead_update"
    priority = Column(Enum(TaskPriority, name="task_priority"), default=TaskPriority.COLD, nullable=False)
    status = Column(Enum(TaskStatus, name="task_status"), default=TaskStatus.PENDING, nullable=False, index=True)
    attempt_count = Column(Integer, default=0, nullable=False)
    max_attempts = Column(Integer, default=5, nullable=False)
    payload = Column(EncryptedJSONB, default=dict, nullable=False)
    result = Column(EncryptedJSONB, default=dict, nullable=False)
    error_message = Column(Text, nullable=True)
    next_retry_at = Column(DateTime(timezone=True), nullable=True, index=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
