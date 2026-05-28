from __future__ import annotations

import argparse
import asyncio
import sys

from loguru import logger

from aq_scraper.config.settings import Settings
from aq_scraper.scrapers.openaq import OpenAQScraper


async def main() -> None:
    parser = argparse.ArgumentParser(description="Air Quality Data Scraper")
    parser.add_argument(
        "--location-ids",
        default=None,
        help="Comma-separated OpenAQ location IDs (overrides defaults)",
    )
    parser.add_argument(
        "--date-from",
        default=None,
        help="Start date ISO 8601 (e.g. 2021-01-01T00:00:00Z)",
    )
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    settings = Settings()

    location_ids: list[str] | None = None
    if args.location_ids:
        location_ids = [x.strip() for x in args.location_ids.split(",")]
    elif settings.OPENAQ_LOCATION_IDS:
        location_ids = [x.strip() for x in settings.OPENAQ_LOCATION_IDS.split(",")]

    date_from = args.date_from or settings.OPENAQ_DATE_FROM

    scraper = OpenAQScraper(
        settings,
        location_ids=location_ids,
        date_from=date_from,
    )

    try:
        count = await scraper.run()
        if count > 0:
            logger.info("Scraping complete — {} records written to data/", count)
        else:
            logger.warning("No records found")
    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
