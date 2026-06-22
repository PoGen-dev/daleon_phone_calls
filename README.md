# Mango Calls QA Microservices

Асинхронный Python-проект для обработки звонков MANGO OFFICE через микросервисную архитектуру:

1. `mango-worker` периодически опрашивает API Mango, сохраняет сырой звонок в PostgreSQL/MongoDB и публикует задачу в Kafka.
2. `transcriber-worker` забирает задачу, скачивает аудио, вызывает `gpt-4o-transcribe`/`gpt-transcribe`-совместимую модель OpenAI, сохраняет транскрипт и публикует событие.
3. `quality-worker` оценивает качество разговора через OpenAI LLM в строгий JSON, сохраняет результат в PostgreSQL/MongoDB.
4. `api` даёт health-check и чтение результата по `call_id`.

## Быстрый старт

```bash
cp .env.example .env
# заполните OPENAI_API_KEY, MANGO_API_KEY, MANGO_API_SALT
# при необходимости уточните MANGO_* endpoint'ы под ваш кабинет/API-версию

docker compose up --build
```

Проверка:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/calls/<call_id>
```

## Сервисы

| Сервис | Назначение |
|---|---|
| `api` | FastAPI для проверки состояния и чтения результата |
| `mango-worker` | polling Mango API, сохранение звонков, Kafka task |
| `transcriber-worker` | скачивание аудио, OpenAI transcription, сохранение транскрипта |
| `quality-worker` | LLM-оценка качества разговора, сохранение оценки |
| `postgres` | реляционные индексы, статусы, транскрипции, оценки |
| `mongodb` | сырые JSON-пayload'ы и полные документы |
| `kafka` | передача задач между воркерами |

## Kafka topics

- `mango.calls.raw` — сырой обнаруженный звонок.
- `calls.to_transcribe` — задача на транскрибацию.
- `calls.transcribed` — событие о готовом транскрипте.
- `calls.quality` — событие о готовой оценке.
- `calls.dead_letter` — ошибки обработки.

## Важные переменные окружения

Смотрите `.env.example`.

Особенно важные:

- `MANGO_API_BASE_URL` — по умолчанию `https://app.mango-office.ru/vpbx`.
- `MANGO_STATS_REQUEST_ENDPOINT` — по умолчанию `stats/request`.
- `MANGO_STATS_RESULT_ENDPOINT` — по умолчанию `stats/result`.
- `MANGO_STATS_FIELDS` — поля CSV-выгрузки. Добавьте поля с recording URL/ID, если они отличаются в вашем кабинете.
- `MANGO_RECORDING_DOWNLOAD_ENDPOINT` — шаблон endpoint'а для скачивания записи по ID, например `records/{recording_id}`. Если Mango отдаёт прямой `recording_url`, он будет использован напрямую.
- `OPENAI_TRANSCRIBE_MODEL` — по умолчанию `gpt-4o-transcribe`. Можно поставить нужную вам `gpt-transcribe`-модель, если она доступна в аккаунте.
- `OPENAI_QUALITY_MODEL` — модель для оценки качества.

## Mango integration notes

Клиент Mango реализован как адаптер:

- запросы отправляются `POST application/x-www-form-urlencoded` с полями `vpbx_api_key`, `sign`, `json`;
- `sign = sha256(api_key + json + api_salt)`;
- `stats/request` создаёт выгрузку;
- `stats/result` опрашивается до готовности CSV/JSON результата;
- CSV парсится по `MANGO_STATS_FIELDS`.

Если ваша версия Mango отдаёт звонки вебхуками, можно добавить отдельный HTTP-ingest сервис и публиковать тот же `CallDiscoveredEvent` в `calls.to_transcribe`; downstream-воркеры менять не нужно.

## Локальная разработка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest
python -m compileall app
```

## Производственные замечания

- Для настоящего production желательно вынести миграции в Alembic, включить TLS/SASL для Kafka, секреты хранить во внешнем secret store.
- В Postgres лежат нормализованные индексы и статусы, в Mongo — полные сырые документы для аудита.
- Воркер Mango использует cursor в таблице `worker_state` и небольшой lookback, поэтому безопасен к перезапускам и частично перекрывает задержки Mango API.
- Идемпотентность обеспечивается `ON CONFLICT` по `call_id` и повторной проверкой наличия транскрипта/оценки.
