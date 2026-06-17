import pytest
import json
from cryptography.fernet import Fernet
from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.orm import declarative_base, sessionmaker
from src.utils.encryption import EncryptedJSONB
from src.config import settings

# Declare a test base for SQLAlchemy models
Base = declarative_base()


class MockModel(Base):
    __tablename__ = "mock_model"
    id = Column(Integer, primary_key=True)
    encrypted_data = Column(EncryptedJSONB)


def test_encryption_decryption_direct():
    """Verify that EncryptedJSONB process_bind_param and process_result_value work correctly."""
    key = settings.ENCRYPTION_KEY
    decorator = EncryptedJSONB()

    test_dict = {"hello": "world", "nested": {"key": 123}}

    # Test process_bind_param (encryption)
    encrypted_str = decorator.process_bind_param(test_dict, None)
    assert isinstance(encrypted_str, str)
    assert encrypted_str != json.dumps(test_dict)  # Ensure it is encrypted

    # Test decrypting it back
    decrypted_dict = decorator.process_result_value(encrypted_str, None)
    assert decrypted_dict == test_dict


def test_backward_compatibility():
    """Verify that plaintext data reads correctly without errors (fallback mode)."""
    decorator = EncryptedJSONB()

    # Case A: JSON string
    test_dict = {"foo": "bar"}
    plaintext_str = json.dumps(test_dict)
    decrypted = decorator.process_result_value(plaintext_str, None)
    assert decrypted == test_dict

    # Case B: Python dict directly (as returned by some SQLAlchemy/SQLite dialects)
    decrypted_dict = decorator.process_result_value(test_dict, None)
    assert decrypted_dict == test_dict

    # Case C: None value
    assert decorator.process_bind_param(None, None) is None
    assert decorator.process_result_value(None, None) is None


def test_invalid_decrypt_returns_value():
    """Verify that values that fail decryption completely are returned as-is instead of crashing."""
    decorator = EncryptedJSONB()
    malformed_str = "random_garbage_non_json_non_fernet"
    
    res = decorator.process_result_value(malformed_str, None)
    assert res == malformed_str


def test_database_integration():
    """Verify end-to-end encryption/decryption using SQLite in-memory database."""
    # Create SQLite engine
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_base_all = Base.metadata.create_all
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()

    test_payload = {"user": "alice", "transcript": [{"speaker": "bot", "content": "hi"}]}

    # Insert mock record
    record = MockModel(id=1, encrypted_data=test_payload)
    session.add(record)
    session.commit()
    session.close()

    from sqlalchemy import text
    # Query directly via raw SQL connection to assert that the value stored in the database is ciphertext
    with engine.connect() as conn:
        result = conn.execute(text("SELECT encrypted_data FROM mock_model WHERE id = 1")).first()
        raw_val = result[0]
        # Assert stored value is indeed encrypted string (meaning it doesn't look like plaintext JSON)
        assert isinstance(raw_val, str)
        assert "alice" not in raw_val
        assert "hi" not in raw_val

    # Read record back using SQLAlchemy session and assert it decrypts automatically
    session = Session()
    queried_record = session.query(MockModel).filter_by(id=1).first()
    assert queried_record.encrypted_data == test_payload
    session.close()
