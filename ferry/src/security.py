import json
import secrets
from pathlib import Path
import hmac
import hashlib
import time
from fastapi import HTTPException
from pydantic import BaseModel


class ClientSecret(BaseModel):
    client_id: str
    client_secret: str


class SecretsManager:
    SECRETS_FILE = Path(__file__).parent.parent / ".ferry" / "server_secrets.json"

    @classmethod
    def _ensure_secrets_dir(cls):
        cls.SECRETS_FILE.parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    def credentials_exist(cls) -> bool:
        """Check if credentials exist."""
        return cls.SECRETS_FILE.exists() and cls.SECRETS_FILE.stat().st_size > 0

    @classmethod
    def generate_secret(cls) -> ClientSecret:
        """Generate new credentials and store them."""
        cls._ensure_secrets_dir()
        creds = ClientSecret(
            client_id=f"ferry-client{secrets.token_urlsafe(16)}",
            client_secret=secrets.token_urlsafe(32),
        )
        with open(cls.SECRETS_FILE, "w") as f:
            json.dump(creds.model_dump(), f)
        cls.SECRETS_FILE.chmod(0o600)
        return creds

    @classmethod
    def get_credentials(cls) -> ClientSecret:
        """Retrieve current credentials."""
        if not cls.credentials_exist():
            raise ValueError("No credentials found")
        with open(cls.SECRETS_FILE, "r") as f:
            return ClientSecret(**json.load(f))

    @classmethod
    def verify_request(cls, client_id: str, timestamp: str, signature: str, body: bytes):
        """Verify HMAC signature of incoming request."""
        creds = cls.get_credentials()
        if creds.client_id != client_id:
            raise HTTPException(401, "Unknown client ID")
        try:
            if abs(time.time() - int(timestamp)) > 300:
                raise HTTPException(401, "Timestamp expired or invalid")
        except ValueError:
            raise HTTPException(401, "Invalid timestamp format")
        message = f"{timestamp}.{body.decode()}"
        expected_sig = hmac.new(
            creds.client_secret.encode(), message.encode(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(signature, expected_sig):
            raise HTTPException(401, "Signature mismatch")
