from __future__ import annotations

import asyncio
import io
from datetime import timedelta
from urllib.parse import urlparse, urlunparse

from minio import Minio

from app.common.config import Settings


class MinioStorage:
    def __init__(self, settings: Settings) -> None:
        self.bucket = settings.minio_bucket
        self.secure = settings.minio_secure
        self.public_base_url = settings.minio_public_base_url
        self.presigned_expires = timedelta(seconds=settings.minio_presigned_url_expires_seconds)
        self.client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key.get_secret_value(),
            secret_key=settings.minio_secret_key.get_secret_value(),
            secure=settings.minio_secure,
        )

    async def ensure_bucket(self) -> None:
        exists = await asyncio.to_thread(self.client.bucket_exists, self.bucket)
        if not exists:
            await asyncio.to_thread(self.client.make_bucket, self.bucket)

    async def upload(self, object_name: str, data: bytes, *, content_type: str = "audio/mpeg") -> None:
        await self.ensure_bucket()
        stream = io.BytesIO(data)
        await asyncio.to_thread(
            self.client.put_object,
            self.bucket,
            object_name,
            stream,
            len(data),
            content_type=content_type,
        )

    async def download(self, object_name: str) -> bytes:
        response = await asyncio.to_thread(self.client.get_object, self.bucket, object_name)
        try:
            return await asyncio.to_thread(response.read)
        finally:
            await asyncio.to_thread(response.close)
            await asyncio.to_thread(response.release_conn)

    async def remove(self, object_name: str) -> None:
        await asyncio.to_thread(self.client.remove_object, self.bucket, object_name)

    async def presigned_download_url(self, object_name: str) -> str:
        url = await asyncio.to_thread(
            self.client.presigned_get_object,
            self.bucket,
            object_name,
            expires=self.presigned_expires,
        )
        return self._with_public_base_url(url)

    def _with_public_base_url(self, url: str) -> str:
        if not self.public_base_url:
            return url

        base = self.public_base_url.rstrip("/")
        if "://" not in base:
            scheme = "https" if self.secure else "http"
            base = f"{scheme}://{base}"

        parsed_url = urlparse(url)
        parsed_base = urlparse(base)
        base_path = parsed_base.path.rstrip("/")
        return urlunparse(
            (
                parsed_base.scheme,
                parsed_base.netloc,
                f"{base_path}{parsed_url.path}",
                "",
                parsed_url.query,
                parsed_url.fragment,
            )
        )

    async def is_available(self) -> bool:
        try:
            await asyncio.to_thread(self.client.bucket_exists, self.bucket)
            return True
        except Exception:
            return False
