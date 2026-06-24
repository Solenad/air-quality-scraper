#!/usr/bin/env python3
"""Cron entrypoint for Render scheduler.

Syncs CSVs with Cloudflare R2, runs the OpenAQ scraper with
incremental resume, then syncs updated CSVs back.

R2 config (env vars, all required for cloud sync):
    R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger

from aq_scraper.__main__ import main as scraper_main
from scripts.r2_sync import R2Sync


def _sync_data_dir() -> Path:
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def run() -> None:
    data_dir = _sync_data_dir()

    r2 = R2Sync.from_env()
    if r2:
        r2.download_csvs(data_dir)
    else:
        logger.info("No R2 — running without persistent storage")

    try:
        asyncio.run(scraper_main())
    except Exception:
        logger.exception("Scraper failed")
        raise

    if r2:
        r2.upload_csvs(data_dir)


if __name__ == "__main__":
    run()
