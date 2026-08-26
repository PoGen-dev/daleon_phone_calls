#!/usr/bin/env bash

set -Eeuo pipefail

# ============================================================
# SETTINGS
# ============================================================

FREEZE="${FREEZE:-1}"
INCLUDE_AUDIO="${INCLUDE_AUDIO:-1}"

PGUSER_DUMP="${PGUSER_DUMP:-app}"
PGDB_DUMP="${PGDB_DUMP:-calls}"

STAMP="$(date +%Y%m%d_%H%M%S)"
ROOT="$(pwd)"
DUMP_DIR="${ROOT}/system_dump_${STAMP}"

MINIO_BUCKET_DUMP="${MINIO_BUCKET_DUMP:-mango-calls}"

if [[ -f ".env" ]]; then
    ENV_MINIO_BUCKET="$(
        awk '
        index($0, "=") {
            key=substr($0,1,index($0,"=")-1)
            if (key == "MINIO_BUCKET") {
                print substr($0,index($0,"=")+1)
                exit
            }
        }' .env | tr -d '\r"'\'
    )"

    if [[ -n "${ENV_MINIO_BUCKET}" ]]; then
        MINIO_BUCKET_DUMP="${ENV_MINIO_BUCKET}"
    fi
fi

mkdir -p \
    "${DUMP_DIR}/host" \
    "${DUMP_DIR}/docker" \
    "${DUMP_DIR}/logs" \
    "${DUMP_DIR}/postgres/csv" \
    "${DUMP_DIR}/kafka/topics" \
    "${DUMP_DIR}/minio" \
    "${DUMP_DIR}/git" \
    "${DUMP_DIR}/config" \
    "${DUMP_DIR}/source" \
    "${DUMP_DIR}/api"


echo "============================================================"
echo "FULL SYSTEM DUMP"
echo "============================================================"
echo "Destination: ${DUMP_DIR}"
echo "FREEZE=${FREEZE}"
echo "INCLUDE_AUDIO=${INCLUDE_AUDIO}"
echo


# ============================================================
# 1. HOST INFORMATION
# ============================================================

date -Is > "${DUMP_DIR}/host/date.txt" || true
uname -a > "${DUMP_DIR}/host/uname.txt" || true
uptime > "${DUMP_DIR}/host/uptime.txt" || true
df -h > "${DUMP_DIR}/host/df_h.txt" || true
df -i > "${DUMP_DIR}/host/df_i.txt" || true
free -h > "${DUMP_DIR}/host/free.txt" || true
lsblk > "${DUMP_DIR}/host/lsblk.txt" || true
mount > "${DUMP_DIR}/host/mount.txt" || true
timedatectl > "${DUMP_DIR}/host/timedatectl.txt" 2>&1 || true

ps auxf > "${DUMP_DIR}/host/processes.txt" || true

if command -v ss >/dev/null 2>&1; then
    ss -lntup > "${DUMP_DIR}/host/listening_ports.txt" 2>&1 || true
fi

if command -v journalctl >/dev/null 2>&1; then
    journalctl -u docker --no-pager \
        > "${DUMP_DIR}/host/docker_daemon.log" 2>&1 || true

    journalctl -k --no-pager \
        > "${DUMP_DIR}/host/kernel.log" 2>&1 || true
fi

dmesg -T > "${DUMP_DIR}/host/dmesg.txt" 2>&1 || true


# ============================================================
# 2. DOCKER STATE BEFORE FREEZE
# ============================================================

docker --version \
    > "${DUMP_DIR}/docker/docker_version.txt" 2>&1 || true

docker compose version \
    > "${DUMP_DIR}/docker/compose_version.txt" 2>&1 || true

docker info \
    > "${DUMP_DIR}/docker/docker_info.txt" 2>&1 || true

docker system df -v \
    > "${DUMP_DIR}/docker/docker_system_df.txt" 2>&1 || true

docker compose ps -a --no-trunc \
    > "${DUMP_DIR}/docker/compose_ps_before.txt" 2>&1 || true

docker compose images \
    > "${DUMP_DIR}/docker/compose_images.txt" 2>&1 || true

docker images --digests --no-trunc \
    > "${DUMP_DIR}/docker/images.txt" 2>&1 || true

docker volume ls \
    > "${DUMP_DIR}/docker/volumes.txt" 2>&1 || true

docker network ls \
    > "${DUMP_DIR}/docker/networks.txt" 2>&1 || true

docker stats --no-stream --all \
    > "${DUMP_DIR}/docker/stats_before.txt" 2>&1 || true


# ============================================================
# 3. API / PYTHON INFORMATION BEFORE FREEZE
# ============================================================

curl -fsS http://localhost:8080/health \
    > "${DUMP_DIR}/api/health_before.json" 2>&1 || true

curl -fsS http://localhost:8080/openapi.json \
    > "${DUMP_DIR}/api/openapi.json" 2>&1 || true

docker compose exec -T api python -V \
    > "${DUMP_DIR}/config/python_version.txt" 2>&1 || true

docker compose exec -T api pip freeze \
    > "${DUMP_DIR}/config/pip_freeze.txt" 2>&1 || true


# ============================================================
# 4. CONFIGURATION WITHOUT SECRETS
# ============================================================

if [[ -f ".env" ]]; then

    awk '
    {
        line=$0

        if (line ~ /^[[:space:]]*#/ || index(line,"=") == 0) {
            print line
            next
        }

        pos=index(line,"=")
        key=substr(line,1,pos-1)
        lkey=tolower(key)

        if (
            lkey ~ /api_key/ ||
            lkey ~ /api_salt/ ||
            lkey ~ /token/ ||
            lkey ~ /secret/ ||
            lkey ~ /password/ ||
            lkey ~ /postgres_dsn/ ||
            lkey ~ /proxy_url/ ||
            lkey ~ /mango_accounts/ ||
            lkey ~ /access_key/
        ) {
            print key "=<REDACTED>"
        } else {
            print line
        }
    }
    ' .env > "${DUMP_DIR}/config/env.sanitized"
fi


docker compose config 2>/dev/null | \
awk '
{
    low=tolower($0)

    if (
        low ~ /api_key/ ||
        low ~ /api_salt/ ||
        low ~ /token:/ ||
        low ~ /secret/ ||
        low ~ /password/ ||
        low ~ /postgres_dsn/ ||
        low ~ /proxy_url/ ||
        low ~ /mango_accounts/ ||
        low ~ /access_key/
    ) {
        sub(/:.*/, ": <REDACTED>")
    }

    print
}
' > "${DUMP_DIR}/config/docker-compose.sanitized.yaml" || true


# ============================================================
# 5. GIT INFORMATION
# ============================================================

git rev-parse HEAD \
    > "${DUMP_DIR}/git/commit.txt" 2>&1 || true

git branch --show-current \
    > "${DUMP_DIR}/git/branch.txt" 2>&1 || true

git status --porcelain=v1 \
    > "${DUMP_DIR}/git/status.txt" 2>&1 || true

git log --all --decorate --oneline -100 \
    > "${DUMP_DIR}/git/log.txt" 2>&1 || true

git diff \
    > "${DUMP_DIR}/git/diff.patch" 2>&1 || true

git diff --cached \
    > "${DUMP_DIR}/git/diff_cached.patch" 2>&1 || true

git remote -v \
    > "${DUMP_DIR}/git/remotes.txt" 2>&1 || true


# ============================================================
# 6. CURRENT PROJECT SOURCE
#
# Deliberately exclude credentials.
# ============================================================

tar \
    --exclude='./.git' \
    --exclude='./.env' \
    --exclude='./infra/mihomo/config.yaml' \
    --exclude='./.venv' \
    --exclude='./venv' \
    --exclude='./__pycache__' \
    --exclude='./.pytest_cache' \
    --exclude='./system_dump_*' \
    -czf "${DUMP_DIR}/source/project_current.tar.gz" \
    .


# ============================================================
# 7. DOCKER INSPECT
# ============================================================

mapfile -t CONTAINER_IDS < <(docker compose ps -aq 2>/dev/null || true)

if (( ${#CONTAINER_IDS[@]} > 0 )); then

    docker inspect "${CONTAINER_IDS[@]}" \
        > "${DUMP_DIR}/docker/containers_inspect.json" 2>&1 || true

    for CID in "${CONTAINER_IDS[@]}"; do

        NAME="$(docker inspect --format '{{.Name}}' "$CID" 2>/dev/null | sed 's#^/##')"

        [[ -z "${NAME}" ]] && NAME="${CID}"

        docker diff "$CID" \
            > "${DUMP_DIR}/docker/diff_${NAME}.txt" 2>&1 || true

    done
fi


# ============================================================
# 8. LOGS
# ============================================================

echo "Collecting Docker logs..."

docker compose logs --no-color --timestamps \
    > "${DUMP_DIR}/logs/all_services.log" 2>&1 || true

mapfile -t SERVICES < <(docker compose config --services 2>/dev/null || true)

for SERVICE in "${SERVICES[@]}"; do

    echo "  logs: ${SERVICE}"

    docker compose logs \
        --no-color \
        --timestamps \
        "${SERVICE}" \
        > "${DUMP_DIR}/logs/${SERVICE}.log" 2>&1 || true

done


# ============================================================
# 9. FREEZE APPLICATION WORKERS
#
# PostgreSQL/Kafka/MinIO remain running.
# This gives a substantially more consistent snapshot.
# ============================================================

declare -a RESTART_SERVICES=()

if [[ "${FREEZE}" == "1" ]]; then

    echo
    echo "Freezing application workers..."

    mapfile -t RUNNING_SERVICES < <(
        docker compose ps --status running --services 2>/dev/null || true
    )

    for SERVICE in \
        api \
        mango-worker \
        transcriber-worker \
        quality-worker \
        telegram-worker
    do

        if printf '%s\n' "${RUNNING_SERVICES[@]}" | grep -Fxq "${SERVICE}"; then

            echo "  stopping: ${SERVICE}"

            docker compose stop "${SERVICE}"

            RESTART_SERVICES+=("${SERVICE}")
        fi

    done
fi


restore_services() {

    if (( ${#RESTART_SERVICES[@]} > 0 )); then

        echo
        echo "Restarting application services..."

        for SERVICE in "${RESTART_SERVICES[@]}"; do
            echo "  starting: ${SERVICE}"
            docker compose start "${SERVICE}" || true
        done

        RESTART_SERVICES=()
    fi
}

trap restore_services EXIT


# ============================================================
# 10. POSTGRESQL FULL DUMP
# ============================================================

echo
echo "Dumping PostgreSQL..."

docker compose exec -T postgres \
    pg_dump \
    -U "${PGUSER_DUMP}" \
    -d "${PGDB_DUMP}" \
    -Fc \
    > "${DUMP_DIR}/postgres/calls.dump"


docker compose exec -T postgres \
    pg_dump \
    -U "${PGUSER_DUMP}" \
    -d "${PGDB_DUMP}" \
    --no-owner \
    --no-acl \
    > "${DUMP_DIR}/postgres/calls.sql"


docker compose exec -T postgres \
    pg_dump \
    -U "${PGUSER_DUMP}" \
    -d "${PGDB_DUMP}" \
    --schema-only \
    --no-owner \
    --no-acl \
    > "${DUMP_DIR}/postgres/schema.sql"


docker compose exec -T postgres \
    pg_dumpall \
    -U "${PGUSER_DUMP}" \
    --globals-only \
    > "${DUMP_DIR}/postgres/globals.sql" 2>&1 || true


docker compose exec -T postgres \
    psql \
    -U "${PGUSER_DUMP}" \
    -d "${PGDB_DUMP}" \
    -c '\dt+' \
    > "${DUMP_DIR}/postgres/tables.txt" 2>&1 || true


docker compose exec -T postgres \
    psql \
    -U "${PGUSER_DUMP}" \
    -d "${PGDB_DUMP}" \
    -c '\di+' \
    > "${DUMP_DIR}/postgres/indexes.txt" 2>&1 || true


docker compose exec -T postgres \
    psql \
    -U "${PGUSER_DUMP}" \
    -d "${PGDB_DUMP}" \
    -c "
SELECT
    relname AS table_name,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    n_live_tup,
    n_dead_tup,
    seq_scan,
    idx_scan
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC;
" > "${DUMP_DIR}/postgres/table_stats.txt" 2>&1 || true


docker compose exec -T postgres \
    psql \
    -U "${PGUSER_DUMP}" \
    -d "${PGDB_DUMP}" \
    -c "
SELECT *
FROM pg_stat_database
ORDER BY datname;
" > "${DUMP_DIR}/postgres/database_stats.txt" 2>&1 || true


docker compose exec -T postgres \
    psql \
    -U "${PGUSER_DUMP}" \
    -d "${PGDB_DUMP}" \
    -c "
SELECT
    pid,
    usename,
    application_name,
    client_addr,
    state,
    wait_event_type,
    wait_event,
    query_start,
    LEFT(query, 500) AS query
FROM pg_stat_activity;
" > "${DUMP_DIR}/postgres/activity.txt" 2>&1 || true


# ============================================================
# 11. EXPORT EVERY PUBLIC TABLE TO CSV
# ============================================================

mapfile -t TABLES < <(
    docker compose exec -T postgres \
        psql \
        -U "${PGUSER_DUMP}" \
        -d "${PGDB_DUMP}" \
        -Atc "
SELECT tablename
FROM pg_tables
WHERE schemaname='public'
ORDER BY tablename;
" | tr -d '\r'
)


for TABLE in "${TABLES[@]}"; do

    [[ -z "${TABLE}" ]] && continue

    echo "  PostgreSQL CSV: ${TABLE}"

    docker compose exec -T postgres \
        psql \
        -U "${PGUSER_DUMP}" \
        -d "${PGDB_DUMP}" \
        -c "\\copy \"${TABLE}\" TO STDOUT WITH CSV HEADER" \
        > "${DUMP_DIR}/postgres/csv/${TABLE}.csv"

done


# ============================================================
# 12. EXACT TABLE COUNTS
# ============================================================

: > "${DUMP_DIR}/postgres/table_counts.txt"

for TABLE in "${TABLES[@]}"; do

    [[ -z "${TABLE}" ]] && continue

    COUNT="$(
        docker compose exec -T postgres \
            psql \
            -U "${PGUSER_DUMP}" \
            -d "${PGDB_DUMP}" \
            -Atc "SELECT COUNT(*) FROM \"${TABLE}\";" \
            | tr -d '\r'
    )"

    printf "%-40s %s\n" "${TABLE}" "${COUNT}" \
        >> "${DUMP_DIR}/postgres/table_counts.txt"

done


# ============================================================
# 13. KAFKA METADATA
# ============================================================

echo
echo "Dumping Kafka metadata..."

docker compose exec -T kafka \
    kafka-topics \
    --bootstrap-server kafka:9092 \
    --list \
    > "${DUMP_DIR}/kafka/topics.txt" 2>&1 || true


docker compose exec -T kafka \
    kafka-topics \
    --bootstrap-server kafka:9092 \
    --describe \
    > "${DUMP_DIR}/kafka/topics_describe.txt" 2>&1 || true


docker compose exec -T kafka \
    kafka-consumer-groups \
    --bootstrap-server kafka:9092 \
    --list \
    > "${DUMP_DIR}/kafka/consumer_groups.txt" 2>&1 || true


docker compose exec -T kafka \
    kafka-consumer-groups \
    --bootstrap-server kafka:9092 \
    --all-groups \
    --describe \
    > "${DUMP_DIR}/kafka/consumer_groups_describe.txt" 2>&1 || true


# ============================================================
# 14. KAFKA MESSAGES
# ============================================================

mapfile -t TOPICS < <(
    docker compose exec -T kafka \
        kafka-topics \
        --bootstrap-server kafka:9092 \
        --list 2>/dev/null \
        | tr -d '\r' \
        | grep -v '^__' || true
)


for TOPIC in "${TOPICS[@]}"; do

    [[ -z "${TOPIC}" ]] && continue

    SAFE_TOPIC="$(echo "${TOPIC}" | tr '/:' '__')"

    echo "  Kafka topic: ${TOPIC}"

    docker compose exec -T kafka \
        kafka-console-consumer \
        --bootstrap-server kafka:9092 \
        --topic "${TOPIC}" \
        --from-beginning \
        --timeout-ms 10000 \
        > "${DUMP_DIR}/kafka/topics/${SAFE_TOPIC}.jsonl" \
        2> "${DUMP_DIR}/kafka/topics/${SAFE_TOPIC}.stderr" \
        || true

done


# ============================================================
# 15. MINIO METADATA
# ============================================================

echo
echo "Dumping MinIO..."

docker compose exec -T minio \
    mc ls \
    --recursive \
    --json \
    "local/${MINIO_BUCKET_DUMP}" \
    > "${DUMP_DIR}/minio/objects.jsonl" 2>&1 || true


docker compose exec -T minio \
    mc du \
    "local/${MINIO_BUCKET_DUMP}" \
    > "${DUMP_DIR}/minio/size.txt" 2>&1 || true


# ============================================================
# 16. FULL MINIO AUDIO COPY
# ============================================================

if [[ "${INCLUDE_AUDIO}" == "1" ]]; then

    echo "Copying MinIO objects..."

    MINIO_CID="$(docker compose ps -q minio)"

    if [[ -n "${MINIO_CID}" ]]; then

        docker exec "${MINIO_CID}" \
            rm -rf /tmp/mango_system_dump || true

        docker exec "${MINIO_CID}" \
            mkdir -p /tmp/mango_system_dump

        docker exec "${MINIO_CID}" \
            mc mirror \
            --overwrite \
            "local/${MINIO_BUCKET_DUMP}" \
            /tmp/mango_system_dump

        mkdir -p "${DUMP_DIR}/minio/${MINIO_BUCKET_DUMP}"

        docker cp \
            "${MINIO_CID}:/tmp/mango_system_dump/." \
            "${DUMP_DIR}/minio/${MINIO_BUCKET_DUMP}/"

        docker exec "${MINIO_CID}" \
            rm -rf /tmp/mango_system_dump || true
    fi

fi


# ============================================================
# 17. RESTORE APPLICATION
# ============================================================

restore_services
trap - EXIT

sleep 3


docker compose ps -a --no-trunc \
    > "${DUMP_DIR}/docker/compose_ps_after.txt" 2>&1 || true

curl -fsS http://localhost:8080/health \
    > "${DUMP_DIR}/api/health_after.json" 2>&1 || true


# ============================================================
# 18. FINAL SUMMARY
# ============================================================

{
    echo "Created: $(date -Is)"
    echo
    echo "Project root:"
    echo "${ROOT}"
    echo
    echo "PostgreSQL:"
    echo "  database=${PGDB_DUMP}"
    echo "  user=${PGUSER_DUMP}"
    echo
    echo "MinIO:"
    echo "  bucket=${MINIO_BUCKET_DUMP}"
    echo "  include_audio=${INCLUDE_AUDIO}"
    echo
    echo "Freeze workers:"
    echo "  ${FREEZE}"
    echo
    echo "Dump size:"
    du -sh "${DUMP_DIR}"
} > "${DUMP_DIR}/SUMMARY.txt"


# ============================================================
# 19. INTEGRITY MANIFEST
# ============================================================

(
    cd "${DUMP_DIR}"

    find . \
        -type f \
        ! -name MANIFEST.sha256 \
        -print0 \
        | sort -z \
        | xargs -0 sha256sum
) > "${DUMP_DIR}/MANIFEST.sha256"


# ============================================================
# 20. FINAL ARCHIVE
# ============================================================

ARCHIVE="${ROOT}/system_dump_${STAMP}.tar.gz"

echo
echo "Creating archive..."

tar \
    -C "$(dirname "${DUMP_DIR}")" \
    -czf "${ARCHIVE}" \
    "$(basename "${DUMP_DIR}")"


echo
echo "============================================================"
echo "DONE"
echo "============================================================"
echo
echo "Dump directory:"
echo "${DUMP_DIR}"
echo
echo "Archive:"
echo "${ARCHIVE}"
echo
du -sh "${DUMP_DIR}" "${ARCHIVE}"
echo