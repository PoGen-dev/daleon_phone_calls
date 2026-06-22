from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "local"
    log_level: str = "INFO"

    postgres_dsn: str = "postgresql://app:app@postgres:5432/calls"
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_group_prefix: str = "mango-calls-local"
    retry_max_attempts: int = Field(default=3, ge=1)
    retry_backoff_seconds: float = Field(default=2.0, ge=0)
    outbox_batch_size: int = Field(default=100, ge=1)

    minio_endpoint: str = "minio:9000"
    minio_access_key: SecretStr = Field(default=SecretStr("minioadmin"))
    minio_secret_key: SecretStr = Field(default=SecretStr("minioadmin"))
    minio_bucket: str = "mango-calls"
    minio_secure: bool = False

    openrouter_api_key: SecretStr = Field(default=SecretStr(""))
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    openrouter_http_referer: str | None = None
    openrouter_app_name: str = "Mango Transcribe Analysis"
    openai_transcribe_model: str = "openai/gpt-4o-transcribe"
    openai_quality_model: str = "openai/gpt-4o-mini"
    openai_quality_temperature: float = 0.0

    mango_api_base_url: str = "https://app.mango-office.ru/vpbx"
    mango_api_key: SecretStr = Field(default=SecretStr(""))
    mango_api_salt: SecretStr = Field(default=SecretStr(""))
    mango_stats_request_endpoint: str = "stats/request"
    mango_stats_result_endpoint: str = "stats/result"
    mango_recording_download_endpoint: str = "queries/recording/post/"
    mango_poll_interval_seconds: int = 30
    mango_catchup_interval_seconds: float = Field(default=1.0, ge=0)
    mango_lookback_seconds: int = 300
    mango_request_window_seconds: int = 300
    mango_result_poll_attempts: int = 60
    mango_result_poll_interval_seconds: int = 5
    mango_worker_concurrency: int = Field(default=4, ge=1)
    mango_stats_fields: Annotated[str, Field(description="Comma-separated Mango stats fields")] = (
        "records,start,finish,answer,from_extension,from_number,to_extension,"
        "to_number,disconnect_reason,line_number,location,entry_id"
    )
    mango_default_timezone: str = "Europe/Moscow"

    telegram_bot_token: SecretStr = Field(default=SecretStr(""))
    telegram_chat_ids: str = ""
    telegram_error_bot_token: SecretStr = Field(default=SecretStr(""))
    telegram_error_chat_ids: str = ""
    telegram_api_base_url: str = "https://api.telegram.org"

    topic_mango_raw: str = "mango.calls.raw"
    topic_to_transcribe: str = "calls.to_transcribe"
    topic_to_analyze: str = "calls.to_analyze"
    topic_to_notify: str = "calls.to_notify"
    topic_dead_letter: str = "calls.dead_letter"

    api_host: str = "0.0.0.0"
    api_port: int = 8080

    @property
    def mango_fields_list(self) -> list[str]:
        return [item.strip() for item in self.mango_stats_fields.split(",") if item.strip()]

    @staticmethod
    def _chat_ids(value: str) -> list[str]:
        return list(dict.fromkeys(item.strip() for item in value.split(",") if item.strip()))

    @property
    def telegram_main_chat_ids(self) -> list[str]:
        return self._chat_ids(self.telegram_chat_ids)

    @property
    def telegram_failure_chat_ids(self) -> list[str]:
        return self._chat_ids(self.telegram_error_chat_ids)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
