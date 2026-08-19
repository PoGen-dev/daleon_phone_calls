from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


async def normalize_phone_audio(
    audio: bytes,
    *,
    enabled: bool,
    timeout_seconds: float,
) -> tuple[bytes, str, dict[str, object]]:
    """Normalize phone-call audio for STT and fall back to the original bytes.

    Output is 16 kHz mono PCM WAV. The filter chain removes very low/high
    frequencies outside the useful speech band and normalizes loudness.
    """
    if not enabled:
        return audio, "recording", {"enabled": False, "applied": False}

    process: asyncio.subprocess.Process | None = None
    try:
        process = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            "pipe:0",
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-af",
            "highpass=f=70,lowpass=f=7600,loudnorm=I=-16:LRA=11:TP=-1.5",
            "-c:a",
            "pcm_s16le",
            "-f",
            "wav",
            "pipe:1",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            process.communicate(input=audio), timeout=timeout_seconds
        )
        if process.returncode != 0 or not stdout:
            message = stderr.decode("utf-8", errors="replace")[-1000:]
            raise RuntimeError(
                f"ffmpeg exited with code {process.returncode}: {message}"
            )
        return (
            stdout,
            "recording.normalized.wav",
            {
                "enabled": True,
                "applied": True,
                "source_bytes": len(audio),
                "normalized_bytes": len(stdout),
                "sample_rate": 16000,
                "channels": 1,
            },
        )
    except (FileNotFoundError, OSError, RuntimeError, asyncio.TimeoutError) as exc:
        if process is not None and process.returncode is None:
            process.kill()
            await process.wait()
        logger.warning("Audio preprocessing failed; using original audio: %s", exc)
        return (
            audio,
            "recording",
            {
                "enabled": True,
                "applied": False,
                "fallback": True,
                "error": str(exc),
            },
        )
