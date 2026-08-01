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
        openai_classification_model="openai/gpt-4o-mini",
        mango_enrich_user_metadata=False,
        telegram_bot_token=SecretStr("main-token"),
        telegram_chat_ids="main-chat,main-chat-2",
        telegram_error_bot_token=SecretStr("error-token"),
        telegram_error_chat_ids="error-chat,error-chat-2",
        telegram_admin_chat_id="admin-chat",
        telegram_admin_critical_thread_id="11",
        telegram_admin_closed_deals_thread_id="12",
        telegram_admin_other_thread_id="13",
        telegram_admin_transcripts_thread_id="14",
        telegram_toyota_nissan_chat_id="tn-chat",
        telegram_toyota_nissan_critical_thread_id="21",
        telegram_toyota_nissan_closed_deals_thread_id="22",
        telegram_toyota_nissan_other_thread_id="23",
        telegram_volvo_vag_chat_id="vv-chat",
        telegram_volvo_vag_critical_thread_id="31",
        telegram_volvo_vag_closed_deals_thread_id="32",
        telegram_volvo_vag_other_thread_id="33",
        retry_backoff_seconds=0,
        mango_recording_download_interval_seconds=0,
    )
