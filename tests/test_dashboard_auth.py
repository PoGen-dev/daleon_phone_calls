from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from app.common.dashboard_auth import (
    DashboardAuthConfigurationError,
    DashboardAuthManager,
)


def _settings(**overrides):
    values = {
        "dashboard_auth_enabled": True,
        "dashboard_admin_username": "admin",
        "dashboard_admin_password": SecretStr("very-strong-password"),
        "dashboard_session_secret": SecretStr("s" * 64),
        "dashboard_session_ttl_seconds": 3600,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_dashboard_auth_rejects_weak_configuration() -> None:
    with pytest.raises(DashboardAuthConfigurationError):
        DashboardAuthManager.from_settings(
            _settings(dashboard_admin_password=SecretStr("admin"))
        )
    with pytest.raises(DashboardAuthConfigurationError):
        DashboardAuthManager.from_settings(
            _settings(dashboard_session_secret=SecretStr("short"))
        )


def test_dashboard_auth_session_roundtrip_and_expiry() -> None:
    auth = DashboardAuthManager.from_settings(_settings())
    assert auth.authenticate("admin", "very-strong-password") is True
    assert auth.authenticate("admin", "wrong-password") is False
    assert auth.authenticate("other", "very-strong-password") is False

    token, issued = auth.issue_session(now=1000)
    verified = auth.verify_session(token, now=1001)
    assert verified is not None
    assert verified.username == "admin"
    assert verified.expires_at == issued.expires_at
    assert auth.verify_session(token, now=issued.expires_at) is None


def test_dashboard_auth_rejects_tampered_token() -> None:
    auth = DashboardAuthManager.from_settings(_settings())
    token, _ = auth.issue_session(now=1000)
    payload, signature = token.split(".", 1)
    assert auth.verify_session(f"{payload}x.{signature}", now=1001) is None


def test_dashboard_auth_can_be_explicitly_disabled() -> None:
    auth = DashboardAuthManager.from_settings(
        _settings(
            dashboard_auth_enabled=False,
            dashboard_admin_password=SecretStr(""),
            dashboard_session_secret=SecretStr(""),
        )
    )
    assert auth.verify_session(None, now=1000) is not None
