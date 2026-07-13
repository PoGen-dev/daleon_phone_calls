from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
from urllib.parse import urlparse

import httpx


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def masked(value: str) -> str:
    if not value:
        return "<empty>"
    if len(value) <= 10:
        return value[:2] + "***"
    return value[:6] + "***" + value[-4:]


def masked_proxy(proxy_url: str | None) -> str:
    if not proxy_url:
        return "<direct or environment proxy>"
    parsed = urlparse(proxy_url)
    if not parsed.username and not parsed.password:
        return proxy_url
    auth = parsed.username or ""
    if parsed.password:
        auth += ":***"
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{auth}@{host}{port}"


async def check_openrouter(proxy_url: str | None) -> bool:
    api_key = env("OPENROUTER_API_KEY")
    base_url = env("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").rstrip("/")
    app_name = env("OPENROUTER_APP_NAME", "Mango Transcribe Analysis")
    referer = env("OPENROUTER_HTTP_REFERER")

    if not api_key or api_key == "replace-me":
        print("OPENROUTER: SKIP - OPENROUTER_API_KEY is empty")
        return False

    headers = {
        "Authorization": f"Bearer {api_key}",
        "X-Title": app_name,
    }
    if referer:
        headers["HTTP-Referer"] = referer

    print(f"OPENROUTER: url={base_url}/models proxy={masked_proxy(proxy_url)} key={masked(api_key)}")
    try:
        async with httpx.AsyncClient(proxy=proxy_url, timeout=30.0) as client:
            response = await client.get(f"{base_url}/models", headers=headers)
            body = response.text[:500]
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:
        print(f"OPENROUTER: FAIL - {type(exc).__name__}: {exc}")
        if isinstance(exc, httpx.HTTPStatusError):
            print(f"OPENROUTER: response={exc.response.text[:1000]}")
        return False

    models = payload.get("data") if isinstance(payload, dict) else None
    count = len(models) if isinstance(models, list) else "unknown"
    print(f"OPENROUTER: OK - status={response.status_code} models={count}")
    if body and not isinstance(models, list):
        print(f"OPENROUTER: body={body}")
    return True


async def check_telegram(proxy_url: str | None, *, error_bot: bool = False) -> bool:
    token_name = "TELEGRAM_ERROR_BOT_TOKEN" if error_bot else "TELEGRAM_BOT_TOKEN"
    token = env(token_name)
    base_url = env("TELEGRAM_API_BASE_URL", "https://api.telegram.org").rstrip("/")
    label = "TELEGRAM_ERROR" if error_bot else "TELEGRAM"

    if not token or token == "replace-me":
        print(f"{label}: SKIP - {token_name} is empty")
        return False

    url = f"{base_url}/bot{token}/getMe"
    print(f"{label}: url={base_url}/bot***/getMe proxy={masked_proxy(proxy_url)} token={masked(token)}")
    try:
        async with httpx.AsyncClient(proxy=proxy_url, timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:
        print(f"{label}: FAIL - {type(exc).__name__}: {exc}")
        if isinstance(exc, httpx.HTTPStatusError):
            print(f"{label}: response={exc.response.text[:1000]}")
        return False

    if not payload.get("ok"):
        print(f"{label}: FAIL - response={payload}")
        return False

    bot = payload.get("result") or {}
    print(f"{label}: OK - @{bot.get('username', '<unknown>')} id={bot.get('id', '<unknown>')}")
    return True


async def main() -> int:
    parser = argparse.ArgumentParser(description="Test OpenRouter and Telegram access through proxy.")
    parser.add_argument("--env-file", default=".env", help="Path to .env file")
    parser.add_argument("--openrouter-proxy", default=None, help="Proxy URL for OpenRouter")
    parser.add_argument("--telegram-proxy", default=None, help="Proxy URL for Telegram")
    parser.add_argument("--with-error-bot", action="store_true", help="Also test TELEGRAM_ERROR_BOT_TOKEN")
    args = parser.parse_args()

    load_dotenv(Path(args.env_file))

    openrouter_proxy = (
        args.openrouter_proxy
        or env("OPENROUTER_PROXY_URL")
        or env("HTTPS_PROXY")
        or env("HTTP_PROXY")
        or None
    )
    telegram_proxy = (
        args.telegram_proxy
        or env("TELEGRAM_PROXY_URL")
        or env("HTTPS_PROXY")
        or env("HTTP_PROXY")
        or None
    )

    results = [
        await check_openrouter(openrouter_proxy),
        await check_telegram(telegram_proxy),
    ]
    if args.with_error_bot:
        results.append(await check_telegram(telegram_proxy, error_bot=True))

    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))