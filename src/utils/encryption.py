import json
import logging
from cryptography.fernet import Fernet
from sqlalchemy.types import TypeDecorator, JSON

from src.config import settings

logger = logging.getLogger(__name__)


class EncryptedJSONB(TypeDecorator):
    """
    SQLAlchemy TypeDecorator that encrypts a JSON dictionary at rest
    using Fernet (AES-128 in CBC mode with SHA256 HMAC).
    Falls back to plaintext JSON if decryption fails (for backward compatibility).
    """
    impl = JSON
    cache_ok = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Initialize Fernet with the encryption key from config
        self.fernet = Fernet(settings.ENCRYPTION_KEY.encode())

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        try:
            # Serialize python structure to json, then encrypt
            json_str = json.dumps(value)
            encrypted_bytes = self.fernet.encrypt(json_str.encode("utf-8"))
            # Storing the encrypted ciphertext as a JSON-encoded string
            return encrypted_bytes.decode("utf-8")
        except Exception as e:
            logger.exception("Failed to encrypt JSON column value")
            # Fall back to returning value as-is to avoid data loss
            return value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        
        # If stored as a string (representing the ciphertext)
        if isinstance(value, str):
            try:
                decrypted_bytes = self.fernet.decrypt(value.encode("utf-8"))
                return json.loads(decrypted_bytes.decode("utf-8"))
            except Exception:
                # If decryption fails, the string might be a raw JSON string
                try:
                    return json.loads(value)
                except Exception:
                    return value
        
        # If database parsed it directly as a dict/list (already plaintext)
        return value
