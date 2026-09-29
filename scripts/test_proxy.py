from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from urllib.parse import urlparse

import httpx


from app.common.config import Settings


def masked(value: str) -> str:
    if not value:
        return "<empty>"
    if len(value) <= 10:
        return value[:2] + "***"
    return value[:6] + "***" + value[-4:]


def masked_proxy(proxy_url: str | None) -> str:
    if not proxy_url:
        return "<direct>"
    parsed = urlparse(proxy_url)
    auth = ""
    if parsed.username:
        auth = parsed.username
        if parsed.password is not None:
            auth += ":***"
        auth += "@"
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{auth}{host}{port}"


async def check_exit_ip(proxy_url: str | None) -> bool:
    print(f"EXIT_IP: proxy={masked_proxy(proxy_url)}")
    try:
        async with httpx.AsyncClient(
            proxy=proxy_url,
            timeout=20.0,
            trust_env=False,
        ) as client:
            response = await client.get("https://api.ipify.org?format=json")
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:
        print(f"EXIT_IP: FAIL - {type(exc).__name__}: {exc}")
        return False

    print(f"EXIT_IP: OK - {payload.get('ip', '<unknown>')}")
    return True
 


async def check_openrouter(settings: Settings, proxy_url: str | None) -> bool:
    api_key = settings.openrouter_api_key.get_secret_value()
    base_url = settings.openrouter_base_url.rstrip("/")

    if not api_key or api_key == "replace-me":
        print("OPENROUTER: SKIP - OPENROUTER_API_KEY is empty")
        return False

    headers = {
        "Authorization": f"Bearer {api_key}",
        "X-Title": settings.openrouter_app_name,
    }
    if settings.openrouter_http_referer:
        headers["HTTP-Referer"] = settings.openrouter_http_referer

    print(
        f"OPENROUTER: url={base_url}/models "
        f"proxy={masked_proxy(proxy_url)} key={masked(api_key)}"
    )
    try:
        async with httpx.AsyncClient(
            proxy=proxy_url,
            timeout=30.0,
            trust_env=False,
        ) as client:
            response = await client.get(f"{base_url}/models", headers=headers)
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
    return True


async def check_telegram(
    settings: Settings,
    proxy_url: str | None,
    *,
    error_bot: bool = False,
) -> bool:
    token = (
        settings.telegram_error_bot_token.get_secret_value()
        if error_bot
        else settings.telegram_bot_token.get_secret_value()
    )
    token_name = "TELEGRAM_ERROR_BOT_TOKEN" if error_bot else "TELEGRAM_BOT_TOKEN"
    label = "TELEGRAM_ERROR" if error_bot else "TELEGRAM"

    if not token or token == "replace-me":
        print(f"{label}: SKIP - {token_name} is empty")
        return False

    base_url = settings.telegram_api_base_url.rstrip("/")
    url = f"{base_url}/bot{token}/getMe"
    print(
        f"{label}: url={base_url}/bot***/getMe "
        f"proxy={masked_proxy(proxy_url)} token={masked(token)}"
    )
    try:
        async with httpx.AsyncClient(
            proxy=proxy_url,
            timeout=30.0,
            trust_env=False,
        ) as client:
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
    parser = argparse.ArgumentParser(
        description="Test Amnezia SOCKS5 routing for OpenRouter and Telegram."
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env file")
    parser.add_argument(
        "--openrouter-proxy",
        default=None,
        help="Temporary OpenRouter proxy URL override for this test",
    )
    parser.add_argument(
        "--telegram-proxy",
        default=None,
        help="Temporary Telegram proxy URL override for this test",
    )
    parser.add_argument(
        "--with-error-bot",
        action="store_true",
        help="Also test TELEGRAM_ERROR_BOT_TOKEN",
    )
    args = parser.parse_args()

    settings = Settings(_env_file=Path(args.env_file))
    try:
        openrouter_proxy = args.openrouter_proxy or settings.openrouter_effective_proxy_url
        telegram_proxy = args.telegram_proxy or settings.telegram_effective_proxy_url
    except ValueError as exc:
        print(f"AMNEZIA: INVALID CONFIG - {exc}")
        return 2

    print(f"AMNEZIA: enabled={settings.amnezia_socks5_enabled}")
    print(f"OPENROUTER_PROXY: {masked_proxy(openrouter_proxy)}")
    print(f"TELEGRAM_PROXY:   {masked_proxy(telegram_proxy)}")

    results = [await check_exit_ip(openrouter_proxy)]
    results.append(await check_openrouter(settings, openrouter_proxy))
    results.append(await check_telegram(settings, telegram_proxy))

    if args.with_error_bot:
        results.append(await check_telegram(settings, telegram_proxy, error_bot=True))

    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))