from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.common.audio import normalize_phone_audio


@pytest.mark.asyncio
async def test_audio_preprocessing_can_be_disabled() -> None:
    audio = b"source"
    result, filename, raw = await normalize_phone_audio(
        audio, enabled=False, timeout_seconds=10
    )
    assert result == audio and filename == "recording"
    assert raw == {"enabled": False, "applied": False}


@pytest.mark.asyncio
async def test_audio_preprocessing_returns_normalized_wav(monkeypatch) -> None:
    process = SimpleNamespace(
        returncode=0,
        communicate=AsyncMock(return_value=(b"RIFF0000WAVEclean", b"")),
    )
    create = AsyncMock(return_value=process)
    monkeypatch.setattr("app.common.audio.asyncio.create_subprocess_exec", create)

    result, filename, raw = await normalize_phone_audio(
        b"source", enabled=True, timeout_seconds=10
    )

    assert result == b"RIFF0000WAVEclean"
    assert filename == "recording.normalized.wav"
    assert raw["applied"] is True and raw["sample_rate"] == 16000
    assert "ffmpeg" == create.await_args.args[0]


@pytest.mark.asyncio
async def test_audio_preprocessing_falls_back_on_ffmpeg_error(monkeypatch) -> None:
    process = SimpleNamespace(
        returncode=1,
        communicate=AsyncMock(return_value=(b"", b"bad input")),
    )
    create = AsyncMock(return_value=process)
    monkeypatch.setattr("app.common.audio.asyncio.create_subprocess_exec", create)

    result, filename, raw = await normalize_phone_audio(
        b"source", enabled=True, timeout_seconds=10
    )

    assert result == b"source" and filename == "recording"
    assert raw["fallback"] is True and raw["applied"] is False
