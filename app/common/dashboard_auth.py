from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass
from typing import Any

from app.common.config import Settings

COOKIE_NAME = "daleon_dashboard_session"
_TOKEN_VERSION = 1


class DashboardAuthConfigurationError(RuntimeError):
    pass


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


@dataclass(frozen=True, slots=True)
class DashboardSession:
    username: str
    issued_at: int
    expires_at: int


class DashboardAuthManager:
    def __init__(
        self,
        *,
        enabled: bool,
        username: str,
        password: str,
        session_secret: str,
        ttl_seconds: int,
    ) -> None:
        self.enabled = enabled
        self.username = username.strip()
        self._password = password
        self._secret = session_secret.encode("utf-8")
        self.ttl_seconds = ttl_seconds
        self.cookie_name = COOKIE_NAME

    @classmethod
    def from_settings(cls, settings: Settings) -> "DashboardAuthManager":
        username = settings.dashboard_admin_username.strip()
        password = settings.dashboard_admin_password.get_secret_value()
        secret = settings.dashboard_session_secret.get_secret_value()

        if settings.dashboard_auth_enabled:
            if not username:
                raise DashboardAuthConfigurationError(
                    "DASHBOARD_AUTH_ENABLED=true but DASHBOARD_ADMIN_USERNAME is empty"
                )
            weak_passwords = {
                "admin",
                "password",
                "change-me",
                "change-me-now",
                "replace-me",
                "replace-with-strong-password",
            }
            if len(password) < 12 or password.strip().lower() in weak_passwords:
                raise DashboardAuthConfigurationError(
                    "DASHBOARD_ADMIN_PASSWORD must be at least 12 characters and must not use a default placeholder"
                )
            weak_secrets = {
                "change-me",
                "replace-me",
                "replace-with-64-char-random-secret",
            }
            if len(secret) < 32 or secret.strip().lower() in weak_secrets:
                raise DashboardAuthConfigurationError(
                    "DASHBOARD_SESSION_SECRET must be at least 32 characters; generate one with: openssl rand -hex 32"
                )

        return cls(
            enabled=settings.dashboard_auth_enabled,
            username=username or "admin",
            password=password,
            session_secret=secret or "disabled-dashboard-auth",
            ttl_seconds=settings.dashboard_session_ttl_seconds,
        )

    def authenticate(self, username: str, password: str) -> bool:
        if not self.enabled:
            return True
        return secrets.compare_digest(
            username.strip(), self.username
        ) and secrets.compare_digest(password, self._password)

    def issue_session(self, *, now: int | None = None) -> tuple[str, DashboardSession]:
        issued_at = int(time.time() if now is None else now)
        session = DashboardSession(
            username=self.username,
            issued_at=issued_at,
            expires_at=issued_at + self.ttl_seconds,
        )
        payload: dict[str, Any] = {
            "v": _TOKEN_VERSION,
            "sub": session.username,
            "iat": session.issued_at,
            "exp": session.expires_at,
        }
        encoded = _b64encode(
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        )
        signature = _b64encode(
            hmac.new(self._secret, encoded.encode("ascii"), hashlib.sha256).digest()
        )
        return f"{encoded}.{signature}", session

    def verify_session(
        self, token: str | None, *, now: int | None = None
    ) -> DashboardSession | None:
        if not self.enabled:
            current = int(time.time() if now is None else now)
            return DashboardSession(self.username, current, current + self.ttl_seconds)
        if not token or "." not in token:
            return None
        encoded, signature = token.rsplit(".", 1)
        expected = _b64encode(
            hmac.new(self._secret, encoded.encode("ascii"), hashlib.sha256).digest()
        )
        if not secrets.compare_digest(signature, expected):
            return None
        try:
            payload = json.loads(_b64decode(encoded))
            version = int(payload["v"])
            username = str(payload["sub"])
            issued_at = int(payload["iat"])
            expires_at = int(payload["exp"])
        except (
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
            binascii.Error,
            UnicodeDecodeError,
        ):
            return None
        current = int(time.time() if now is None else now)
        if version != _TOKEN_VERSION:
            return None
        if not secrets.compare_digest(username, self.username):
            return None
        if issued_at > current + 60 or expires_at <= current or expires_at <= issued_at:
            return None
        if expires_at - issued_at > self.ttl_seconds:
            return None
        return DashboardSession(username, issued_at, expires_at)


__all__ = [
    "COOKIE_NAME",
    "DashboardAuthConfigurationError",
    "DashboardAuthManager",
    "DashboardSession",
]
