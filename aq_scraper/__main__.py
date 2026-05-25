from __future__ import annotations

import argparse
import asyncio
import sys

from loguru import logger

from aq_scraper.config.settings import Settings
from aq_scraper.pipeline.exporter import export_csv
from aq_scraper.scrapers.openaq import OpenAQScraper
from aq_scraper.storage.db import create_engine, create_session_factory, get_session, init_db


async def main() -> None:
    parser = argparse.ArgumentParser(description="Air Quality Data Scraper")
    parser.add_argument(
        "--source",
        required=True,
        help="Data source to scrape (e.g., openaq)",
    )
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    settings = Settings()

    # Validate required DB vars
    if not settings.DB_USER or not settings.DB_PASS or not settings.DB_NAME:
        logger.error("DB_USER, DB_PASS, and DB_NAME must be set in environment")
        sys.exit(1)

    # Setup database
    engine = create_engine(settings)
    await init_db(engine)
    session_factory = create_session_factory(engine)

    if args.source == "openaq":
        scraper = OpenAQScraper(settings)
    else:
        logger.error("Unknown source: {}", args.source)
        sys.exit(1)

    try:
        async with get_session(session_factory) as session:
            count = await scraper.run(session)
            if count > 0:
                export_path = await export_csv(args.source, session)
                if export_path:
                    logger.info("CSV exported to {}", export_path)
            else:
                logger.warning("No readings stored — skipping CSV export")
    finally:
        await scraper.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
