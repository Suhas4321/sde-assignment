import uuid
from sqlalchemy import (
    Column,
    DateTime,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from src.models.base import Base


class CustomerConfig(Base):
    __tablename__ = "customer_configs"

    customer_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hot_keywords = Column(
        ARRAY(String(100)),
        nullable=False,
        server_default="{'confirmed','booked','scheduled','appointment','manager','escalate','complaint','urgent','demo','meeting','canceled','refund'}"
    )
    crm_webhook_url = Column(String(500), nullable=True)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
