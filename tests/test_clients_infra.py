from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.clients.minio import MinioStorage
from app.clients.openai_qa import QUALITY_JSON_SCHEMA, OpenAIQaClient
from app.clients.telegram import TelegramClient
from app.common import db, kafka
from app.common.retry import retry_or_dead_letter


class Dumpable:
    def __init__(self, payload):
        self.payload = payload

    def model_dump(self, **_kwargs):
        return self.payload


@pytest.mark.asyncio
async def test_minio_storage_upload_download_and_health(settings, monkeypatch) -> None:
    response = SimpleNamespace(read=MagicMock(return_value=b"audio"), close=MagicMock(), release_conn=MagicMock())
    client = SimpleNamespace(
        bucket_exists=MagicMock(side_effect=[False, True, RuntimeError("down")]),
        make_bucket=MagicMock(),
        put_object=MagicMock(),
        get_object=MagicMock(return_value=response),
    )
    constructor = MagicMock(return_value=client)
    monkeypatch.setattr("app.clients.minio.Minio", constructor)
    storage = MinioStorage(settings)
    await storage.upload("c/a.mp3", b"123", content_type="audio/mpeg")
    client.make_bucket.assert_called_once_with("mango-calls")
    args = client.put_object.call_args.args
    assert args[:2] == ("mango-calls", "c/a.mp3") and args[4] == 3
    assert await storage.download("c/a.mp3") == b"audio"
    response.close.assert_called_once()
    assert await storage.is_available()
    assert not await storage.is_available()
    constructor.assert_called_once()


@pytest.mark.asyncio
async def test_telegram_sends_to_both_channels_and_validates_response(settings) -> None:
    telegram = TelegramClient(settings)
    await telegram.http.aclose()
    ok = SimpleNamespace(raise_for_status=MagicMock(), json=lambda: {"ok": True})
    rejected = SimpleNamespace(raise_for_status=MagicMock(), json=lambda: {"ok": False, "description": "bad"})
    telegram.http = SimpleNamespace(post=AsyncMock(side_effect=[ok, ok, rejected]), aclose=AsyncMock())
    assert telegram.main_chat_ids == ["main-chat", "main-chat-2"]
    assert telegram.error_chat_ids == ["error-chat", "error-chat-2"]
    await telegram.send("main", chat_id="main-chat")
    await telegram.send("error", chat_id="error-chat", error_channel=True)
    assert "/botmain-token/sendMessage" in telegram.http.post.await_args_list[0].args[0]
    assert telegram.http.post.await_args_list[1].kwargs["json"]["chat_id"] == "error-chat"
    with pytest.raises(RuntimeError, match="rejected"):
        await telegram.send("bad", chat_id="main-chat")
    telegram.main_token = ""
    with pytest.raises(RuntimeError, match="not configured"):
        await telegram.send("missing", chat_id="main-chat")
    await telegram.aclose()
    telegram.http.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_openrouter_client_transcribes_and_scores(settings, monkeypatch) -> None:
    quality = {
        "score": 80,
        "risk_level": "warning",
        "risk_reason": "Нет следующего шага",
        "summary": "Обсудили услугу",
        "errors": ["Не назначена дата"],
        "recommendation": "Перезвонить",
        "criteria": {
            "greeting": 90,
            "needs_discovery": 80,
            "urgency": 70,
            "target_action": 80,
            "objection_handling": 75,
            "closing": 50,
        },
    }
    completion = Dumpable({"choices": []})
    completion.choices = [SimpleNamespace(message=SimpleNamespace(content=__import__("json").dumps(quality)))]
    fake = SimpleNamespace(
        audio=SimpleNamespace(
            transcriptions=SimpleNamespace(create=AsyncMock(return_value=Dumpable({"text": "привет"})))
        ),
        chat=SimpleNamespace(completions=SimpleNamespace(create=AsyncMock(return_value=completion))),
        close=AsyncMock(),
    )
    constructor = MagicMock(return_value=fake)
    monkeypatch.setattr("app.clients.openai_qa.AsyncOpenAI", constructor)
    settings.openrouter_http_referer = "https://app.example"
    ai = OpenAIQaClient(settings)
    text, raw = await ai.transcribe(audio=b"audio", filename="call.mp3")
    result, response = await ai.score_quality(transcript="текст")
    assert text == "привет" and raw["text"] == "привет"
    assert result.score == 80 and response == {"choices": []}
    kwargs = constructor.call_args.kwargs
    assert kwargs["base_url"] == settings.openrouter_base_url
    assert kwargs["default_headers"]["HTTP-Referer"] == "https://app.example"
    assert QUALITY_JSON_SCHEMA["schema"]["additionalProperties"] is False
    await ai.aclose()
    fake.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_openrouter_transcription_fallback_and_empty_completion(settings, monkeypatch) -> None:
    transcription = SimpleNamespace(text="fallback")
    completion = SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=None))])
    fake = SimpleNamespace(
        audio=SimpleNamespace(transcriptions=SimpleNamespace(create=AsyncMock(return_value=transcription))),
        chat=SimpleNamespace(completions=SimpleNamespace(create=AsyncMock(return_value=completion))),
        close=AsyncMock(),
    )
    monkeypatch.setattr("app.clients.openai_qa.AsyncOpenAI", MagicMock(return_value=fake))
    ai = OpenAIQaClient(settings)
    assert (await ai.transcribe(audio=b"x", filename="x"))[0] == "fallback"
    with pytest.raises(Exception):
        await ai.score_quality(transcript="x")


@pytest.mark.asyncio
async def test_retry_republishes_then_moves_to_dead_letter(settings, monkeypatch) -> None:
    publish = AsyncMock()
    monkeypatch.setattr(kafka, "publish_json", publish)
    producer = object()
    moved = await retry_or_dead_letter(
        producer=producer,
        settings=settings,
        source_topic="work",
        payload={"call_id": "c", "attempt": 1},
        error=ValueError("bad"),
        service="worker",
    )
    assert not moved and publish.await_args.args[1] == "work"
    assert publish.await_args.args[2]["attempt"] == 2
    publish.reset_mock()
    await retry_or_dead_letter(
        producer=producer,
        settings=settings,
        source_topic="work",
        payload={"call_id": "c", "attempt": "invalid"},
        error=ValueError("bad"),
        service="worker",
    )
    assert publish.await_args.args[2]["attempt"] == 2
    publish.reset_mock()
    moved = await retry_or_dead_letter(
        producer=producer,
        settings=settings,
        source_topic="work",
        payload={"call_id": "c", "attempt": 3},
        error=ValueError("bad"),
        service="worker",
    )
    assert moved and publish.await_args.args[1] == settings.topic_dead_letter
    assert publish.await_args.args[2]["attempts"] == 3


@pytest.mark.asyncio
async def test_kafka_helpers_and_contexts(settings, monkeypatch) -> None:
    producer = SimpleNamespace(start=AsyncMock(), stop=AsyncMock(), send_and_wait=AsyncMock())
    producer_class = MagicMock(return_value=producer)
    monkeypatch.setattr(kafka, "AIOKafkaProducer", producer_class)
    async with kafka.kafka_producer(settings) as yielded:
        await kafka.publish_json(yielded, "topic", {"x": 1}, key="key")
    producer.start.assert_awaited_once()
    producer.stop.assert_awaited_once()
    producer.send_and_wait.assert_awaited_once_with("topic", {"x": 1}, key="key")

    consumer = SimpleNamespace(start=AsyncMock(), stop=AsyncMock(), commit=AsyncMock())
    consumer_class = MagicMock(return_value=consumer)
    monkeypatch.setattr(kafka, "AIOKafkaConsumer", consumer_class)
    async with kafka.kafka_consumer(settings, topic=("one", "two"), group_suffix="test") as yielded:
        assert yielded is consumer
    assert consumer_class.call_args.args == ("one", "two")
    record = SimpleNamespace(topic="one", partition=0, offset=1)
    await kafka.commit_after(record, consumer)
    consumer.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_postgres_context_closes_pool(settings, monkeypatch) -> None:
    pool = SimpleNamespace(close=AsyncMock())
    create_pool = AsyncMock(return_value=pool)
    monkeypatch.setattr(db.asyncpg, "create_pool", create_pool)
    async with db.postgres_pool(settings) as yielded:
        assert yielded is pool
    create_pool.assert_awaited_once_with(dsn=settings.postgres_dsn, min_size=1, max_size=10)
    pool.close.assert_awaited_once()
