from __future__ import annotations

import json
import os
import sys
from urllib.error import URLError
from urllib.request import urlopen

CONTROLLER = os.getenv("MIHOMO_CONTROLLER_URL", "http://127.0.0.1:9090").rstrip("/")
GROUPS = ("OPENROUTER_AUTO", "TELEGRAM_AUTO")


def fetch(path: str) -> dict:
    with urlopen(f"{CONTROLLER}{path}", timeout=5) as response:  # noqa: S310
        return json.load(response)


def main() -> int:
    try:
        proxies = fetch("/proxies").get("proxies", {})
    except (OSError, URLError, ValueError) as exc:
        print(f"Cannot reach Mihomo controller {CONTROLLER}: {exc}", file=sys.stderr)
        return 1

    failed = False
    for group_name in GROUPS:
        group = proxies.get(group_name)
        if not isinstance(group, dict):
            print(f"{group_name}: missing")
            failed = True
            continue
        print(
            f"{group_name}: active={group.get('now') or '-'} "
            f"candidates={','.join(group.get('all') or []) or '-'}"
        )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
