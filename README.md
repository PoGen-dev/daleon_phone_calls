# Mango Transcribe Analysis

Асинхронный pipeline обработки звонков MANGO OFFICE:

1. `mango-worker` опрашивает Mango, проверяет аудио, сохраняет его в MinIO, а метаданные и Kafka outbox — в PostgreSQL.
2. `transcriber-worker` получает объект MinIO из Kafka, отправляет его в Base64 на JSON STT endpoint OpenRouter,
   разделяет реплики по спикерам и отдельным ИИ-запросом классифицирует звонок.
3. `quality-worker` анализирует текст через `gpt-4o-mini`, сохраняет метрики и публикует задачу уведомления.
4. `telegram-worker` отправляет все бизнес-отчёты, включая критические звонки, основным ботом. Только технические
   DLQ-ошибки после третьей попытки отправляются отдельным error-ботом.

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
# OPENROUTER_TRANSCRIBE_READ_TIMEOUT_SECONDS задаёт ожидание ответа STT, по умолчанию 900 секунд
# OPENAI_CLASSIFICATION_MODEL задаёт модель отдельного этапа классификации
# MANGO_RESOLVE_SIP_SERVICE_NUMBER=true автоматически связывает SIP/добавочный с номером линии автосервиса
# MANGO_ENRICH_USER_METADATA=true дополнительно сохраняет полную карточку сотрудника Mango
docker compose up --build -d
docker compose ps
```

### Несколько MANGO API key/salt

Существующая пара `MANGO_API_KEY` / `MANGO_API_SALT` остаётся основным (`primary`) источником и полностью
обратно совместима с уже накопленными данными. Дополнительные кабинеты/группы задаются JSON-массивом
`MANGO_ACCOUNTS`:

```env
MANGO_API_KEY=ключ_основной_АТС
MANGO_API_SALT=salt_основной_АТС
MANGO_ACCOUNTS='[{"name":"second","api_key":"ключ_2","api_salt":"salt_2"},{"name":"third","api_key":"ключ_3","api_salt":"salt_3"}]'
```

`name` — стабильный технический идентификатор источника: латинские буквы/цифры и `._-`, без пробелов.
Для каждой дополнительной АТС worker:

- создаёт отдельный Mango API client и подписывает запросы её собственными `api_key/api_salt`;
- ведёт отдельный cursor в `worker_state` (`mango_worker_cursor:<name>`);
- добавляет `mango_account` и `mango_original_id` в `calls.raw`;
- сохраняет звонок под ID `<name>:<original_mango_id>`, чтобы одинаковые Mango ID из разных АТС не конфликтовали;
- скачивает запись тем же API client, которым был получен звонок.

Основной `MANGO_API_KEY/MANGO_API_SALT` сохраняет прежние ID звонков и прежний cursor `mango_worker_cursor`,
поэтому включение дополнительных источников не создаёт дублей уже загруженных звонков основного кабинета.
Можно также оставить основную пару пустой и описать все источники только через `MANGO_ACCOUNTS`; в этом случае ID
всех звонков будут namespaced.

Интерфейсы после запуска:

- API: `http://localhost:8080`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- Kafka для хоста: `localhost:29092`

## Pipeline и топики

| Topic                 | Producer           | Consumer              | Назначение                  |
| --------------------- | ------------------ | --------------------- | --------------------------- |
| `mango.calls.raw`     | mango-worker       | аудит/внешние системы | Событие обнаружения звонка  |
| `calls.to_transcribe` | mango-worker       | transcriber-worker    | Транскрибация объекта MinIO |
| `calls.to_analyze`    | transcriber-worker | quality-worker        | Анализ текста из PostgreSQL |
| `calls.to_notify`     | quality-worker     | telegram-worker       | Уведомление основным ботом  |
| `calls.dead_letter`   | все воркеры        | telegram-worker       | Ошибка после трёх попыток   |

Топики с тремя partition создаёт одноразовый сервис `kafka-init`. Consumer offsets фиксируются только после успешной
обработки, повторной публикации или переноса в DLQ.

Если настроены `TELEGRAM_ADMIN_CHAT_ID` и `TELEGRAM_ADMIN_TRANSCRIPTS_THREAD_ID`, в тему транскрипций отправляются
два компактных документа вместо длинного сообщения:

- исходный аудиофайл звонка с короткой служебной подписью;
- `<имя_аудио>_transcript.txt` в UTF-8, содержащий только транскрибированный диалог, уже разбитый по ролям.

Аудио и TXT имеют отдельные ключи идемпотентности. Если Telegram принял аудио, но временно отклонил TXT, при retry
повторно отправится только отсутствующий TXT-файл.


## Качество транскрибации и OpenRouter timeout

По умолчанию `transcriber-worker` перед STT нормализует запись через `ffmpeg`: mono PCM WAV 16 kHz, речевой фильтр
70-7600 Hz и loudness normalization. Если `ffmpeg` недоступен или не смог обработать конкретную запись, worker
не теряет звонок и автоматически отправляет в STT исходный аудиофайл.

Настройки:

- `TRANSCRIPTION_PREPROCESS_AUDIO=true` — включить предобработку.
- `TRANSCRIPTION_FFMPEG_TIMEOUT_SECONDS=90` — максимальное время локальной нормализации.
- `OPENROUTER_TRANSCRIBE_TEMPERATURE=0` — детерминированная STT-декодировка.
- `OPENAI_TRANSCRIBE_LANGUAGE=ru` — явный язык распознавания.

Перед отправкой аудио `transcriber-worker` пишет в лог модель, имя файла, формат, размер аудио и текущие timeout-настройки.
Для длинных записей или медленного proxy увеличивайте:

- `OPENROUTER_TRANSCRIBE_CONNECT_TIMEOUT_SECONDS=30` — ожидание подключения.
- `OPENROUTER_TRANSCRIBE_WRITE_TIMEOUT_SECONDS=120` — ожидание отправки JSON/Base64 аудио.
- `OPENROUTER_TRANSCRIBE_READ_TIMEOUT_SECONDS=900` — ожидание ответа STT.
- `OPENROUTER_TRANSCRIBE_POOL_TIMEOUT_SECONDS=30` — ожидание свободного соединения в HTTP ool.

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
- `call_classifications`: тип звонка, причина классификации, критические ошибки, уверенность и сырой ответ модели.
- `quality_scores`: риск, итог, обычные ошибки, рекомендация и шесть метрик.
- `notifications`: ключи идемпотентности Telegram-событий для каждого `chat_id`.
- `worker_state`: cursor опроса Mango.
- `outbox_events`: гарантированная публикация событий первого воркера в Kafka.

Полный результат доступен по `GET /calls/{call_id}`. `GET /health` проверяет PostgreSQL и MinIO.

## Контроль качества ИИ

После STT модель делит текст на реплики `Менеджер`, `Клиент` и `Спикер не определён`. Это семантическая атрибуция,
а не акустическая diarization. В первый запрос дополнительно передаются метаданные звонка (`incoming/outgoing`, внешний
номер, внутренний extension, найденное имя сотрудника), но модель не имеет права переносить эти сведения в текст.
Код разрешает только пунктуацию, границы реплик и роли, затем проверяет полное совпадение всех исходных слов и их порядка.

Если первичная разметка подозрительна — содержит `unknown`, длинный диалог размечен одним участником или почти не
разбит на реплики — выполняется второй review-запрос через `OPENAI_TRANSCRIPT_ROLE_REVIEW_MODEL` (по умолчанию
`openai/gpt-4o`). Более дорогая модель используется только для сомнительных случаев. Если review изменил исходные слова,
он отбрасывается; валидная первичная разметка сохраняется. Если обе попытки нарушили lossless-проверку, сохраняется
исходный STT-текст с ролью `Спикер не определён`.
 

После разделения спикеров выполняется отдельный запрос классификации. Возможные типы: `Запись`, `Продажи`,
`Доставка`, `Консультация`, `Готовая сделка`, `Критическая`. `Консультация` используется как fallback. Для
`Критической` модель обязана вернуть конкретный список `critical_errors`; технические ошибки интеграций к этой
категории не относятся. Этап идемпотентен: при retry сохранённая классификация не запрашивается повторно.

Анализ использует диапазоны оценки 0-25, 26-50, 51-75 и 76-100, веса критериев и дословные
цитаты-доказательства. Код проверяет наличие каждой цитаты в транскрипте и сам пересчитывает итоговый балл. В
`quality_raw.analysis` сохраняются статусы критериев, доказательства, отдельные возражения, этапы их обработки и
согласованный следующий шаг. Исходный STT и результат разметки ролей доступны в `transcription_raw` через
`GET /calls/{call_id}`.


## Поиск сотрудника по SIP или добавочному

В базовой статистике Mango поле `to_number` может содержать внутренний SIP URI, например
`sip:user2@vpbx400100296.mangosip.ru`, а фактический добавочный находится рядом в `to_extension`. Для разового
поиска сотрудника и связанных номеров выполните:

```bash
docker compose run --rm mango-worker python scripts/lookup_mango_user.py 11
docker compose run --rm mango-worker python scripts/lookup_mango_user.py "sip:user2@vpbx400100296.mangosip.ru"
```

Команда выводит ФИО, `user_id`, отдел, должность, мобильный, исходящую линию, `line_id`, SIP-учётки, телефонные
номера и группы. Для определения автосервиса полное enrichment включать не требуется. По умолчанию:

```env
MANGO_ENRICH_USER_METADATA=true
MANGO_RESOLVE_SIP_SERVICE_NUMBER=true
MANGO_USERS_ENDPOINT=config/users/request
MANGO_INCOMING_LINES_ENDPOINT=incominglines
MANGO_USERS_CACHE_TTL_SECONDS=3600
MANGO_INCOMING_LINES_CACHE_TTL_SECONDS=3600
```

Если номер звонка имеет вид `sip:user44@...mangosip.ru`, worker сначала ищет сотрудника по `to_extension` /
`from_extension` или SIP, затем использует его `telephony.line_id`, получает список `/vpbx/incominglines` и связывает
`line_id` с реальным номером ВАТС. Найденные номера сохраняются в
`calls.raw.mango_employee_service_phone_candidates` и участвуют в `SERVICE_BY_PHONE` раньше исходного `to_number`.
Цифры из SIP-домена (`vpbx400100296...`) больше не считаются телефонным номером.

Для автоматического сохранения полной карточки сотрудника дополнительно включите:

```env
MANGO_ENRICH_USER_METADATA=true
```

Справочник сотрудников и входящих линий кэшируется. Если enrichment или line lookup временно недоступен, загрузка
звонков продолжается; просто конкретный звонок может остаться без определённого автосервиса до следующего нового события.
Скрипт `scripts/lookup_mango_user.py` теперь также выводит `service_phone_candidates`.

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
docker compose exec postgres psql -U app -d calls -c "select call_id,call_type,confidence from call_classifications order by created_at desc limit 10"
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
docker compose exec -T postgres psql -U app -d calls < infra/postgres/migrations/004_call_classifications.sql
```

## Production

Перед production-запуском замените стандартные пароли MinIO/PostgreSQL, включите TLS/SASL, храните секреты вне `.env`
и перенесите SQL-схему в миграции. Для строгой атомарности PostgreSQL/Kafka рекомендуется transactional outbox.


## AmneziaVPN SOCKS5

OpenRouter и Telegram направляются через SOCKS5-сервис, установленный на сервере AmneziaVPN. Mango, PostgreSQL, Kafka
и MinIO продолжают работать по обычному прямому маршруту. Локальный контейнер `mihomo` больше не требуется.

Установите SOCKS5-сервис в AmneziaVPN на VPN-сервере и скопируйте из его настроек `host`, `port`, `username` и
`password`. Затем добавьте/обновите значения в `.env`:

```env
AMNEZIA_SOCKS5_ENABLED=true
AMNEZIA_SOCKS5_HOST=203.0.113.10
AMNEZIA_SOCKS5_PORT=1080
AMNEZIA_SOCKS5_USERNAME=replace-me
AMNEZIA_SOCKS5_PASSWORD=replace-me

# Необязательный аварийный/диагностический fallback. При включённой Amnezia она имеет приоритет.
OPENROUTER_PROXY_URL=
TELEGRAM_PROXY_URL=
NO_PROXY=localhost,127.0.0.1,postgres,kafka,zookeeper,minio,api
```

`AMNEZIA_SOCKS5_USERNAME` и `AMNEZIA_SOCKS5_PASSWORD` автоматически URL-encode'ятся приложением, поэтому спецсимволы
в логине/пароле не нужно экранировать вручную. Реальные значения нельзя коммитить в Git.

Поддержка SOCKS обеспечивается зависимостью `httpx[socks]`. После перехода со старого image пересоберите worker'ы:

```bash
docker compose build --no-cache transcriber-worker quality-worker telegram-worker
docker compose up -d --force-recreate transcriber-worker quality-worker telegram-worker
```

После пересоздания worker'ов удалите orphan-контейнер старого `mihomo` без удаления volumes:

```bash
docker compose up -d --remove-orphans
```

Проверка маршрута Amnezia, внешнего IP, OpenRouter и Telegram без отправки пользовательских сообщений:

```bash
python scripts/test_proxy.py --with-error-bot
```

В выводе должны быть `AMNEZIA: enabled=True`, внешний `EXIT_IP` сервера Amnezia и успешные проверки OpenRouter/Telegram.
Если `AMNEZIA_SOCKS5_ENABLED=false`, приложение использует необязательные `OPENROUTER_PROXY_URL` /
`TELEGRAM_PROXY_URL`, а при пустых значениях работает напрямую.


## Как пересобрать весь проект Windows 

```bash
docker compose down -v --remove-orphans
```

```bash
docker compose rm -f
```

```bash
docker image prune -f
```

```bash
docker compose build --no-cache
```

```bash
docker compose pull
```

```bash
docker compose up -d
```


## Как пересобрать весь проект Linux 

```bash
sudo docker compose down -v --remove-orphans
sudo docker compose rm -f
sudo docker image prune -f
sudo docker compose build --no-cache
sudo docker compose pull
sudo docker compose up -d
```

## Web dashboard (React + FastAPI)

В проекте есть аналитический веб-дашборд по историческим звонкам. Backend использует существующий FastAPI/PostgreSQL,
frontend собирается React/Vite и обслуживается отдельным nginx-контейнером `dashboard`.

### Безопасность и вход администратора

Все `/analytics/*` endpoints защищены серверной авторизацией. React не хранит пароль или session token в localStorage:
после успешного входа FastAPI выдаёт подписанную `HttpOnly` cookie с `SameSite=Strict`. Подпись проверяется на каждом
analytics-запросе, включая прослушивание аудио и CSV export. Dashboard nginx наружу проксирует только `/api/auth/*` и
`/api/analytics/*`; остальные operational API routes через dashboard origin недоступны. Порт FastAPI по умолчанию также
публикуется только на `127.0.0.1`.

Защита включена по умолчанию и намеренно не имеет рабочего default password. Перед пересозданием `api` добавьте в `.env`:

```env
API_BIND=127.0.0.1
API_PUBLIC_PORT=8080

DASHBOARD_BIND=127.0.0.1
DASHBOARD_PORT=3000
DASHBOARD_AUTH_ENABLED=true
DASHBOARD_ADMIN_USERNAME=admin
DASHBOARD_ADMIN_PASSWORD=put-a-long-random-password-here
DASHBOARD_SESSION_SECRET=put-a-random-secret-here
DASHBOARD_SESSION_TTL_SECONDS=28800
DASHBOARD_COOKIE_SECURE=false
```

Session secret удобно сгенерировать так:

```bash
openssl rand -hex 32
```

`DASHBOARD_ADMIN_PASSWORD` должен содержать минимум 12 символов. Если auth включён, а password/session secret пустые,
короткие или оставлены стандартными placeholders, API не стартует — это специально сделано, чтобы dashboard не оказался
случайно открыт без защиты.

Для localhost или SSH tunnel оставьте `DASHBOARD_COOKIE_SECURE=false`. Если dashboard публикуется через HTTPS reverse
proxy, установите:

```env
DASHBOARD_COOKIE_SECURE=true
```

### Аналитика

Dashboard показывает и пересчитывает по общим server-side фильтрам:

- KPI по звонкам, среднему score, analysis coverage, critical, сделкам, записям, pipeline errors и длительности;
- изменение ключевых KPI относительно предыдущего периода той же длины;
- быстрые периоды: сегодня / 7 / 30 / 90 дней / вся история;
- воронку `calls → transcription → classification → quality analysis → notification`;
- динамику количества звонков и среднего score по дням;
- распределение по `risk_level`, `call_type`, pipeline status и длительности;
- средние значения шести критериев качества и самый слабый критерий;
- блок «Требуют внимания»: pipeline errors, critical и звонки со score < 60;
- аналитику менеджеров: calls, score, analysis coverage, critical rate, сделки, конверсия, записи, длительность;
- server-side фильтры по периоду, Mango account, менеджеру, типу, риску, статусу, направлению, score,
  длительности, наличию transcription/quality и полнотекстовый поиск;
- server-side сортировку, размер страницы и пагинацию;
- CSV export текущего отфильтрованного среза (до 50 000 строк);
- подробную карточку звонка с вкладками `Обзор`, `Транскрипт`, `Техническое`: аудио, summary, risk reason,
  recommendation, criteria, objections, next step, critical errors, grounding diagnostics и Mango metadata.

По умолчанию dashboard доступен только на localhost хоста:

```env
DASHBOARD_BIND=127.0.0.1
DASHBOARD_PORT=3000
```

Для удалённого доступа без отдельного reverse proxy используйте SSH tunnel:

```bash
ssh -L 3000:127.0.0.1:3000 user@server
```

и откройте `http://127.0.0.1:3000` локально.

### Запуск/обновление

Индексы migration `005_dashboard_indexes.sql` достаточно применить один раз к существующей БД:


```bash
docker compose exec -T postgres psql -U app -d calls < infra/postgres/migrations/005_dashboard_indexes.sql
```

После изменения dashboard/auth пересоберите API и frontend:

```bash
docker compose build api dashboard
docker compose up -d --force-recreate api dashboard
```

Проверка health API с хоста:

```bash
curl -fsS http://127.0.0.1:8080/health
```

Проверка защиты (без cookie должен быть `401`):

```bash
curl -i http://127.0.0.1:3000/api/analytics/filters
```

Проверка login через cookie jar:

```bash
curl -i -c /tmp/daleon-dashboard.cookies \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"YOUR_PASSWORD"}' \
  http://127.0.0.1:3000/api/auth/login

curl -b /tmp/daleon-dashboard.cookies \
  http://127.0.0.1:3000/api/analytics/filters
```

Backend endpoints:

```text
POST /auth/login
GET  /auth/me
POST /auth/logout

GET /analytics/filters
GET /analytics/overview
GET /analytics/calls
GET /analytics/export.csv
GET /analytics/calls/{call_id}
GET /analytics/calls/{call_id}/audio
```

Все агрегаты `/analytics/overview`, список `/analytics/calls` и CSV export используют одинаковый набор фильтров.