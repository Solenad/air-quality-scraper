"""Cloudflare R2 sync for air-quality-scraper CSVs.

Syncs the local data/ directory with an R2 bucket so that
ephemeral cron containers (Render free tier) retain data
between runs.

Usage
-----
    r2 = R2Sync.from_env()
    r2.download_csvs(data_dir)   # before scrape
    # ... run scraper ...
    r2.upload_csvs(data_dir)     # after scrape
"""
from __future__ import annotations

import os
from pathlib import Path

import boto3
from botocore.config import Config
from loguru import logger


class R2Sync:
    def __init__(
        self,
        bucket: str,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
    ) -> None:
        self.bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            config=Config(signature_version="s3v4"),
        )

    @classmethod
    def from_env(cls) -> R2Sync | None:
        endpoint = os.environ.get("R2_ENDPOINT_URL")
        key_id = os.environ.get("R2_ACCESS_KEY_ID")
        secret = os.environ.get("R2_SECRET_ACCESS_KEY")
        bucket = os.environ.get("R2_BUCKET_NAME")

        if not all([endpoint, key_id, secret, bucket]):
            logger.warning("R2 not configured — skipping cloud sync")
            return None

        return cls(
            bucket=bucket,
            endpoint_url=endpoint,
            access_key_id=key_id,
            secret_access_key=secret,
        )

    def download_csvs(self, dest_dir: Path) -> None:
        dest_dir.mkdir(parents=True, exist_ok=True)
        try:
            response = self._client.list_objects_v2(Bucket=self.bucket)
            objects = response.get("Contents", [])
        except Exception as exc:
            logger.warning("R2 list failed: {} — skipping download", exc)
            return

        count = 0
        for obj in objects:
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue
            dest_path = dest_dir / Path(key).name
            try:
                self._client.download_file(self.bucket, key, str(dest_path))
                count += 1
            except Exception as exc:
                logger.warning("R2 download failed for {}: {}", key, exc)

        if count:
            logger.info("R2: downloaded {} CSV(s) to data/", count)

    def upload_csvs(self, src_dir: Path) -> None:
        if not src_dir.is_dir():
            return

        count = 0
        for csv_path in sorted(src_dir.glob("*.csv")):
            try:
                self._client.upload_file(
                    str(csv_path), self.bucket, csv_path.name
                )
                count += 1
            except Exception as exc:
                logger.warning("R2 upload failed for {}: {}", csv_path.name, exc)

        if count:
            logger.info("R2: uploaded {} CSV(s) from data/", count)
