from __future__ import annotations

import pytest
from pydantic import SecretStr

from app.common.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None,
        mango_api_key=SecretStr("mango-key"),
        mango_api_salt=SecretStr("mango-salt"),
        openrouter_api_key=SecretStr("router-key"),
        telegram_bot_token=SecretStr("main-token"),
        telegram_chat_ids="main-chat,main-chat-2",
        telegram_error_bot_token=SecretStr("error-token"),
        telegram_error_chat_ids="error-chat,error-chat-2",
        retry_backoff_seconds=0,
        mango_recording_download_interval_seconds=0,
    )
