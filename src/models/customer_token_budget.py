import uuid
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from src.models.base import Base


class CustomerTokenBudget(Base):
    __tablename__ = "customer_token_budgets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_id = Column(UUID(as_uuid=True), unique=True, nullable=False, index=True)
    tokens_per_minute = Column(Integer, nullable=False)
    requests_per_minute = Column(Integer, nullable=False)
    priority = Column(String(50), default="standard", nullable=False)  # "standard", "premium", "enterprise"
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
