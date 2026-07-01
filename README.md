# Mango Transcribe Analysis

Асинхронный pipeline обработки звонков MANGO OFFICE:

1. `mango-worker` опрашивает Mango, проверяет аудио, сохраняет его в MinIO, а метаданные и Kafka outbox — в PostgreSQL.
2. `transcriber-worker` получает объект MinIO из Kafka, отправляет его в Base64 на JSON STT endpoint OpenRouter и
   сохраняет транскрипцию `gpt-4o-transcribe`.
3. `quality-worker` анализирует текст через `gpt-4o-mini`, сохраняет метрики и публикует задачу уведомления.
4. `telegram-worker` отправляет результат основным ботом, а DLQ после третьей ошибки — отдельным ботом.

Каждый Kafka-task содержит `attempt`. При ошибке задача повторно публикуется в исходный topic; после третьей попытки
создаётся событие `dead_letter`. Первый воркер публикует события через transactional outbox: сохранение звонка и
постановка событий атомарны, а `dedupe_key` не позволяет повторному lookback создавать новые задачи.

## Запуск

```bash
cp .env.example .env
# заполните MANGO_*, OPENROUTER_API_KEY и оба набора TELEGRAM_*
# TELEGRAM_CHAT_IDS и TELEGRAM_ERROR_CHAT_IDS содержат chat_id через запятую
# MANGO_DEFAULT_TIMEZONE задаёт часовой пояс дат в Telegram, по умолчанию Europe/Moscow
# MINIO_PUBLIC_BASE_URL должен быть адресом MinIO, открываемым из Telegram
docker compose up --build -d
docker compose ps
```

Интерфейсы после запуска:

- API: `http://localhost:8080`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- Kafka для хоста: `localhost:29092`

## Pipeline и топики

| Topic | Producer | Consumer | Назначение |
|---|---|---|---|
| `mango.calls.raw` | mango-worker | аудит/внешние системы | Событие обнаружения звонка |
| `calls.to_transcribe` | mango-worker | transcriber-worker | Транскрибация объекта MinIO |
| `calls.to_analyze` | transcriber-worker | quality-worker | Анализ текста из PostgreSQL |
| `calls.to_notify` | quality-worker | telegram-worker | Уведомление основным ботом |
| `calls.dead_letter` | все воркеры | telegram-worker | Ошибка после трёх попыток |

Топики с тремя partition создаёт одноразовый сервис `kafka-init`. Consumer offsets фиксируются только после успешной
обработки, повторной публикации или переноса в DLQ.

## Mango rate limit

Скачивание записей Mango ограничено настройками:

- `MANGO_WORKER_CONCURRENCY=2` — не больше двух звонков одновременно.
- `MANGO_RECORDING_DOWNLOAD_INTERVAL_SECONDS=2` — минимальная пауза между запросами скачивания записи внутри
  `mango-worker`.
- `MANGO_RESULT_POLL_INTERVAL_SECONDS=10` — пауза между проверками готовности отчёта Mango.
- `RETRY_BACKOFF_SECONDS=4` — пауза между повторными попытками задачи.

При HTTP `429 Too Many Requests` клиент ждёт `Retry-After`, если Mango его вернул, иначе использует backoff и повторяет
запрос до передачи ошибки в worker retry.

## Данные

- `calls`: метаданные Mango, статус и путь объекта MinIO.
- `transcriptions`: текст по ролям, исходный STT-текст, модель и результат проверки сохранности слов.
- `quality_scores`: риск, итог, ошибки, рекомендация и шесть метрик.
- `notifications`: ключи идемпотентности Telegram-событий для каждого `chat_id`.
- `worker_state`: cursor опроса Mango.
- `outbox_events`: гарантированная публикация событий первого воркера в Kafka.

Полный результат доступен по `GET /calls/{call_id}`. `GET /health` проверяет PostgreSQL и MinIO.

## Контроль качества ИИ

После STT модель делит текст на реплики `Менеджер`, `Клиент` и `Спикер не определён`. Это семантическая атрибуция,
а не акустическая diarization: код разрешает только пунктуацию и границы реплик, затем проверяет полное совпадение слов
с исходным текстом. При любом добавлении, удалении или перестановке слов сохраняется исходный текст с неизвестной ролью.

Анализ использует диапазоны оценки 0-25, 26-50, 51-75 и 76-100, веса критериев и дословные
цитаты-доказательства. Код проверяет наличие каждой цитаты в транскрипте и сам пересчитывает итоговый балл. В
`quality_raw.analysis` сохраняются статусы критериев, доказательства, отдельные возражения, этапы их обработки и
согласованный следующий шаг. Исходный STT и результат разметки ролей доступны в `transcription_raw` через
`GET /calls/{call_id}`.

## Проверка

```bash
pip install -r requirements-dev.txt
pytest
python -m compileall app
ruff check app tests
```

`pytest` настроен с `--cov-fail-under=90`.

Ручной smoke-test:

```bash
curl -fsS http://localhost:8080/health
docker compose ps
docker compose logs --tail=100 mango-worker transcriber-worker quality-worker telegram-worker
docker compose exec minio mc ls --recursive local/mango-calls
docker compose exec postgres psql -U app -d calls -c "select id,status,audio_object_name from calls order by created_at desc limit 10"
docker compose exec postgres psql -U app -d calls -c "select call_id,left(transcript,80) from transcriptions order by created_at desc limit 10"
docker compose exec postgres psql -U app -d calls -c "select call_id,score,risk_level from quality_scores order by created_at desc limit 10"
docker compose exec postgres psql -U app -d calls -c "select id,topic,attempts,last_error from outbox_events where published_at is null order by id"
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic calls.dead_letter --from-beginning --max-messages 1
```

Одноразовая обработка последнего звонка с записью за предыдущие 7 дней:

```bash
docker compose stop mango-worker
docker compose run --rm -e MANGO_TEST_LATEST_CALL_ONLY=true mango-worker
docker compose start mango-worker
```

Период поиска задаётся переменной `MANGO_TEST_LOOKBACK_SECONDS`. Этот режим не изменяет cursor штатного опроса и
завершается после обработки одного звонка. Идемпотентность сохраняется: уже обработанный звонок не создаёт повторные
задачи и уведомления.

Для повторного создания схемы после изменения `init.sql` удалите локальные volumes: `docker compose down -v`.

Для обновления уже работающей базы без удаления данных выполните:

```bash
docker compose exec -T postgres psql -U app -d calls < infra/postgres/migrations/002_telegram_chat_ids.sql
docker compose exec -T postgres psql -U app -d calls < infra/postgres/migrations/003_outbox.sql
```

## Production

Перед production-запуском замените стандартные пароли MinIO/PostgreSQL, включите TLS/SASL, храните секреты вне `.env`
и перенесите SQL-схему в миграции. Для строгой атомарности PostgreSQL/Kafka рекомендуется transactional outbox.
