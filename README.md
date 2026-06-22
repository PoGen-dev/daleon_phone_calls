# Mango Transcribe Analysis

Асинхронный pipeline обработки звонков MANGO OFFICE:

1. `mango-worker` опрашивает Mango, параллельно сохраняет аудио в MinIO и метаданные в PostgreSQL.
2. `transcriber-worker` получает объект MinIO из Kafka, вызывает `gpt-4o-transcribe` через OpenRouter и сохраняет текст.
3. `quality-worker` анализирует текст через `gpt-4o-mini`, сохраняет метрики и публикует задачу уведомления.
4. `telegram-worker` отправляет результат основным ботом, а DLQ после третьей ошибки — отдельным ботом.

Каждый Kafka-task содержит `attempt`. При ошибке задача повторно публикуется в исходный topic; после третьей попытки
создаётся событие `dead_letter`. Записи в PostgreSQL и уведомления идемпотентны.

## Запуск

```bash
cp .env.example .env
# заполните MANGO_*, OPENROUTER_API_KEY и оба набора TELEGRAM_*
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

## Данные

- `calls`: метаданные Mango, статус и путь объекта MinIO.
- `transcriptions`: текст, модель и сырой ответ транскрибации.
- `quality_scores`: риск, итог, ошибки, рекомендация и шесть метрик.
- `notifications`: ключи идемпотентности отправленных Telegram-событий.
- `worker_state`: cursor опроса Mango.

Полный результат доступен по `GET /calls/{call_id}`. `GET /health` проверяет PostgreSQL и MinIO.

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
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic calls.dead_letter --from-beginning --max-messages 1
```

Для повторного создания схемы после изменения `init.sql` удалите локальные volumes: `docker compose down -v`.

## Production

Перед production-запуском замените стандартные пароли MinIO/PostgreSQL, включите TLS/SASL, храните секреты вне `.env`
и перенесите SQL-схему в миграции. Для строгой атомарности PostgreSQL/Kafka рекомендуется transactional outbox.
