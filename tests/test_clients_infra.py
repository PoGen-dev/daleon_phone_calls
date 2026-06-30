from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.clients.minio import MinioStorage
from app.clients.openai_qa import QUALITY_JSON_SCHEMA, OpenAIQaClient
from app.clients.telegram import TelegramClient
from app.common import db, kafka
from app.common.models import QualityResult
from app.common.retry import retry_or_dead_letter
from app.prompts.quality import QUALITY_SYSTEM_PROMPT


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
        remove_object=MagicMock(),
    )
    constructor = MagicMock(return_value=client)
    monkeypatch.setattr("app.clients.minio.Minio", constructor)
    storage = MinioStorage(settings)
    await storage.upload("c/a.mp3", b"123", content_type="audio/mpeg")
    client.make_bucket.assert_called_once_with("mango-calls")
    args = client.put_object.call_args.args
    assert args[:2] == ("mango-calls", "c/a.mp3") and args[3] == 3
    assert await storage.download("c/a.mp3") == b"audio"
    response.close.assert_called_once()
    await storage.remove("c/a.mp3")
    client.remove_object.assert_called_once_with("mango-calls", "c/a.mp3")
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
        "analysis_confidence": "low",
        "limitations": ["Недостаточно реплик"],
        "criteria": {
            "greeting": 90,
            "needs_discovery": 80,
            "urgency": 70,
            "target_action": 80,
            "objection_handling": 75,
            "closing": 50,
        },
        "criteria_status": {
            "greeting": "not_observed",
            "needs_discovery": "not_observed",
            "urgency": "not_observed",
            "target_action": "not_observed",
            "objection_handling": "not_observed",
            "closing": "not_observed",
        },
        "criteria_evidence": {
            "greeting": [],
            "needs_discovery": [],
            "urgency": [],
            "target_action": [],
            "objection_handling": [],
            "closing": [],
        },
        "objections": [],
        "next_step": {"status": "absent", "quote": None},
    }
    completion = Dumpable({"choices": []})
    completion.choices = [SimpleNamespace(message=SimpleNamespace(content=__import__("json").dumps(quality)))]
    fake = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=AsyncMock(return_value=completion))),
        close=AsyncMock(),
    )
    stt_response = SimpleNamespace(
        raise_for_status=MagicMock(),
        json=lambda: {"text": "привет", "usage": {"seconds": 1}},
    )
    stt_http = SimpleNamespace(post=AsyncMock(return_value=stt_response), aclose=AsyncMock())
    constructor = MagicMock(return_value=fake)
    monkeypatch.setattr("app.clients.openai_qa.AsyncOpenAI", constructor)
    monkeypatch.setattr("app.clients.openai_qa.httpx.AsyncClient", MagicMock(return_value=stt_http))
    settings.openrouter_http_referer = "https://app.example"
    ai = OpenAIQaClient(settings)
    text, raw = await ai.transcribe(audio=b"audio", filename="call.mp3")
    result, response = await ai.score_quality(transcript="текст")
    assert text == "привет" and raw["text"] == "привет"
    assert result.score == 0 and response["quality_control"]["model_score"] == 80
    kwargs = constructor.call_args.kwargs
    assert kwargs["base_url"] == settings.openrouter_base_url
    assert kwargs["default_headers"]["HTTP-Referer"] == "https://app.example"
    stt_payload = stt_http.post.await_args.kwargs["json"]
    assert stt_payload == {
        "model": "openai/gpt-4o-transcribe",
        "input_audio": {"data": "YXVkaW8=", "format": "mp3"},
        "language": "ru",
    }
    assert QUALITY_JSON_SCHEMA["schema"]["additionalProperties"] is False
    await ai.aclose()
    fake.close.assert_awaited_once()
    stt_http.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_openrouter_transcription_fallback_and_empty_completion(settings, monkeypatch) -> None:
    completion = SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=None))])
    fake = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=AsyncMock(return_value=completion))),
        close=AsyncMock(),
    )
    stt_http = SimpleNamespace(
        post=AsyncMock(
            return_value=SimpleNamespace(raise_for_status=MagicMock(), json=lambda: {"text": "fallback"})
        ),
        aclose=AsyncMock(),
    )
    monkeypatch.setattr("app.clients.openai_qa.AsyncOpenAI", MagicMock(return_value=fake))
    monkeypatch.setattr("app.clients.openai_qa.httpx.AsyncClient", MagicMock(return_value=stt_http))
    ai = OpenAIQaClient(settings)
    assert (await ai.transcribe(audio=b"x", filename="x.wav"))[0] == "fallback"
    with pytest.raises(Exception):
        await ai.score_quality(transcript="x")
    await ai.aclose()


@pytest.mark.parametrize(
    ("filename", "audio", "expected"),
    [
        ("recording", b"RIFF0000WAVEaudio", "wav"),
        ("recording.mp3", b"RIFF0000WAVEaudio", "wav"),
        ("recording", b"ID3audio", "mp3"),
        ("recording", b"OggSaudio", "ogg"),
        ("recording", b"fLaCaudio", "flac"),
        ("recording", b"0000ftypaudio", "m4a"),
        ("recording", b"\x1aE\xdf\xa3audio", "webm"),
        ("call.oga", b"audio", "ogg"),
        ("call.mp4", b"audio", "m4a"),
    ],
)
def test_openrouter_detects_audio_format(filename, audio, expected) -> None:
    assert OpenAIQaClient._audio_format(filename, audio) == expected


def test_openrouter_rejects_unknown_audio_format() -> None:
    with pytest.raises(ValueError, match="Cannot determine"):
        OpenAIQaClient._audio_format("recording.bin", b"unknown")


@pytest.mark.asyncio
async def test_transcript_roles_preserve_every_source_word(settings, monkeypatch) -> None:
    content = (
        '{"turns":[{"speaker":"manager","text":"Здравствуйте"},'
        '{"speaker":"client","text":"Мне дорого"}]}'
    )
    completion = Dumpable({"choices": []})
    completion.choices = [SimpleNamespace(message=SimpleNamespace(content=content))]
    fake = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=AsyncMock(return_value=completion))),
        close=AsyncMock(),
    )
    stt_http = SimpleNamespace(aclose=AsyncMock())
    monkeypatch.setattr("app.clients.openai_qa.AsyncOpenAI", MagicMock(return_value=fake))
    monkeypatch.setattr("app.clients.openai_qa.httpx.AsyncClient", MagicMock(return_value=stt_http))
    ai = OpenAIQaClient(settings)

    transcript, raw = await ai.structure_transcript("Здравствуйте. Мне дорого.")

    assert transcript == "Менеджер: Здравствуйте\nКлиент: Мне дорого"
    assert raw["validated"] is True
    await ai.aclose()


@pytest.mark.asyncio
async def test_transcript_roles_fall_back_when_model_invents_words(settings, monkeypatch) -> None:
    content = '{"turns":[{"speaker":"manager","text":"Здравствуйте уважаемый клиент"}]}'
    completion = Dumpable({"choices": []})
    completion.choices = [SimpleNamespace(message=SimpleNamespace(content=content))]
    fake = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=AsyncMock(return_value=completion))),
        close=AsyncMock(),
    )
    stt_http = SimpleNamespace(aclose=AsyncMock())
    monkeypatch.setattr("app.clients.openai_qa.AsyncOpenAI", MagicMock(return_value=fake))
    monkeypatch.setattr("app.clients.openai_qa.httpx.AsyncClient", MagicMock(return_value=stt_http))
    ai = OpenAIQaClient(settings)

    transcript, raw = await ai.structure_transcript("Здравствуйте")

    assert transcript == "Спикер не определён: Здравствуйте"
    assert raw["validated"] is False
    await ai.aclose()


@pytest.mark.asyncio
async def test_transcript_roles_can_be_disabled(settings, monkeypatch) -> None:
    fake = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=AsyncMock())),
        close=AsyncMock(),
    )
    stt_http = SimpleNamespace(aclose=AsyncMock())
    monkeypatch.setattr("app.clients.openai_qa.AsyncOpenAI", MagicMock(return_value=fake))
    monkeypatch.setattr("app.clients.openai_qa.httpx.AsyncClient", MagicMock(return_value=stt_http))
    settings.openai_transcript_role_model = None
    ai = OpenAIQaClient(settings)

    transcript, raw = await ai.structure_transcript("Исходный текст")

    assert transcript == "Спикер не определён: Исходный текст"
    assert raw == {"enabled": False, "validated": True}
    fake.chat.completions.create.assert_not_awaited()
    await ai.aclose()


def test_quality_control_rejects_nonexistent_quotes_and_recalculates_score() -> None:
    payload = {
        "score": 1,
        "risk_level": "warning",
        "risk_reason": "Нет следующего шага",
        "summary": "Клиент считает цену высокой.",
        "errors": ["Не уточнена причина возражения"],
        "recommendation": "Уточнить причину сомнения.",
        "analysis_confidence": "high",
        "limitations": [],
        "criteria": {
            name: 50
            for name in (
                "greeting",
                "needs_discovery",
                "urgency",
                "target_action",
                "objection_handling",
                "closing",
            )
        },
        "criteria_status": {
            "greeting": "observed",
            "needs_discovery": "not_observed",
            "urgency": "not_observed",
            "target_action": "not_observed",
            "objection_handling": "observed",
            "closing": "not_observed",
        },
        "criteria_evidence": {
            "greeting": ["Здравствуйте"],
            "needs_discovery": [],
            "urgency": [],
            "target_action": [],
            "objection_handling": ["Мне дорого"],
            "closing": [],
        },
        "objections": [
            {
                "customer_quote": "Мне дорого",
                "kind": "explicit",
                "category": "price",
                "manager_response_quote": None,
                "completed_steps": [],
                "missing_steps": ["clarified", "answered", "checked_resolution", "agreed_next_step"],
                "resolution": "unresolved",
            }
        ],
        "next_step": {"status": "absent", "quote": None},
    }
    quality = QualityResult.model_validate(payload)
    transcript = "Менеджер: Здравствуйте. Клиент: Мне дорого."

    OpenAIQaClient._validate_quality_evidence(quality, transcript)
    quality = OpenAIQaClient._normalize_criteria_scores(quality)
    assert quality.criteria.needs_discovery == 0
    assert OpenAIQaClient._compute_quality_score(quality) == 15

    payload["criteria_evidence"]["greeting"] = []
    with pytest.raises(ValueError, match="has no evidence"):
        OpenAIQaClient._validate_quality_evidence(QualityResult.model_validate(payload), transcript)

    payload["criteria_evidence"]["greeting"] = ["Здравствуйте"]
    payload["objections"][0]["customer_quote"] = "Такого клиент не говорил"
    with pytest.raises(ValueError, match="unsupported quote"):
        OpenAIQaClient._validate_quality_evidence(QualityResult.model_validate(payload), transcript)


def test_quality_prompt_contains_grounded_sales_rubric() -> None:
    assert "установление контакта" in QUALITY_SYSTEM_PROMPT
    assert "проверил ли удобство разговора" in QUALITY_SYSTEM_PROMPT
    assert "выяснены сроки, дедлайны и срочность" in QUALITY_SYSTEM_PROMPT
    assert "звонок продающий" in QUALITY_SYSTEM_PROMPT
    assert "неотработанный soft_deferral" in QUALITY_SYSTEM_PROMPT
    assert "не утверждай скрытую причину" in QUALITY_SYSTEM_PROMPT


def test_quality_control_rejects_unsupported_critical_risk() -> None:
    payload = {
        "score": 90,
        "risk_level": "critical",
        "risk_reason": "Риск не подтверждён",
        "summary": "Клиент согласился на следующий шаг.",
        "errors": [],
        "recommendation": "Продолжить по договорённости.",
        "criteria": {
            "greeting": 90,
            "needs_discovery": 90,
            "urgency": 80,
            "target_action": 90,
            "objection_handling": 50,
            "closing": 90,
        },
        "analysis_confidence": "high",
        "limitations": [],
        "criteria_status": {
            "greeting": "observed",
            "needs_discovery": "observed",
            "urgency": "observed",
            "target_action": "observed",
            "objection_handling": "not_applicable",
            "closing": "observed",
        },
        "criteria_evidence": {
            "greeting": ["Здравствуйте"],
            "needs_discovery": ["Что нужно сделать"],
            "urgency": ["До пятницы"],
            "target_action": ["Давайте так"],
            "objection_handling": [],
            "closing": ["Давайте так"],
        },
        "objections": [],
        "next_step": {"status": "agreed", "quote": "Давайте так"},
    }
    with pytest.raises(ValueError, match="next step is agreed"):
        OpenAIQaClient._validate_risk_consistency(QualityResult.model_validate(payload))

    payload["next_step"] = {"status": "absent", "quote": None}
    with pytest.raises(ValueError, match="requires an unresolved"):
        OpenAIQaClient._validate_risk_consistency(QualityResult.model_validate(payload))

    payload["objections"] = [
        {
            "customer_quote": "Я подумаю",
            "kind": "soft_deferral",
            "category": "other",
            "manager_response_quote": None,
            "completed_steps": [],
            "missing_steps": ["clarified", "answered", "checked_resolution", "agreed_next_step"],
            "resolution": "unresolved",
        }
    ]
    OpenAIQaClient._validate_risk_consistency(QualityResult.model_validate(payload))


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
    create_pool.assert_awaited_once_with(
        dsn=settings.postgres_dsn,
        min_size=1,
        max_size=10,
        init=db._configure_connection,
    )
    pool.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_postgres_connection_decodes_json() -> None:
    conn = SimpleNamespace(set_type_codec=AsyncMock())
    await db._configure_connection(conn)
    assert [call.args[0] for call in conn.set_type_codec.await_args_list] == ["json", "jsonb"]
    decoder = conn.set_type_codec.await_args.kwargs["decoder"]
    assert decoder('{"cursor":"now"}') == {"cursor": "now"}
