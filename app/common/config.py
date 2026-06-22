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
    mongo_dsn: str = "mongodb://mongo:27017"
    mongo_database: str = "mango_calls"
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_group_prefix: str = "mango-calls-local"

    openai_api_key: SecretStr = Field(default=SecretStr(""))
    openai_transcribe_model: str = "gpt-4o-transcribe"
    openai_quality_model: str = "gpt-4o-mini"
    openai_quality_temperature: float = 0.0

    mango_api_base_url: str = "https://app.mango-office.ru/vpbx"
    mango_api_key: SecretStr = Field(default=SecretStr(""))
    mango_api_salt: SecretStr = Field(default=SecretStr(""))
    mango_stats_request_endpoint: str = "stats/request"
    mango_stats_result_endpoint: str = "stats/result"
    mango_recording_download_endpoint: str = "records/{recording_id}"
    mango_poll_interval_seconds: int = 30
    mango_lookback_seconds: int = 300
    mango_request_window_seconds: int = 300
    mango_result_poll_attempts: int = 12
    mango_result_poll_interval_seconds: int = 5
    mango_stats_fields: Annotated[str, Field(description="Comma-separated Mango stats fields")] = (
        "records,start,finish,from_extension,from_number,to_extension,to_number,disconnect_reason"
    )
    mango_default_timezone: str = "Europe/Moscow"

    topic_mango_raw: str = "mango.calls.raw"
    topic_to_transcribe: str = "calls.to_transcribe"
    topic_transcribed: str = "calls.transcribed"
    topic_quality: str = "calls.quality"
    topic_dead_letter: str = "calls.dead_letter"

    api_host: str = "0.0.0.0"
    api_port: int = 8080

    @property
    def mango_fields_list(self) -> list[str]:
        return [item.strip() for item in self.mango_stats_fields.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
