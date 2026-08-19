from __future__ import annotations

import argparse
import asyncio
import json

from app.clients.mango import MangoClient
from app.common.config import get_settings


async def main(identifier: str) -> int:
    client = MangoClient(get_settings())
    try:
        user = await client.find_user(identifier)
        if user is None:
            print(
                json.dumps(
                    {"identifier": identifier, "found": False},
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 1
        summary = client.employee_summary(user)
        service_phone_candidates = await client.employee_service_phone_candidates(user)
        result = {
            "identifier": identifier,
            "found": True,
            "summary": summary,
            "service_phone_candidates": service_phone_candidates,
            "raw": user,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    finally:
        await client.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find a MANGO OFFICE employee by extension or SIP address."
    )
    parser.add_argument(
        "identifier", help="For example: 11 or sip:user2@vpbx....mangosip.ru"
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main(args.identifier)))
