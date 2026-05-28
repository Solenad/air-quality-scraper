from __future__ import annotations

import asyncio
import csv
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from loguru import logger

from aq_scraper.config.settings import Settings
from aq_scraper.pipeline.cleaner import clean_reading
from aq_scraper.pipeline.validator import validate_reading

OPENAQ_BASE_URL = "https://api.openaq.org/v3"
MAX_RETRIES = 4
RETRY_DELAYS = [1, 2, 4, 8]
DEFAULT_DATE_FROM = "2021-01-01T00:00:00Z"

CSV_FIELDNAMES = [
    "location_name", "location_id", "sensor_id",
    "utc_timestamp", "local_timestamp", "pm25_value", "parameter", "unit",
]

DEFAULT_HISTORICAL_LOCATION_IDS = [
    "4795497", "4797652", "4797660", "4795498", "5975180",
    "4795500", "4795496", "4795499", "6340868", "4797658",
    "4526987", "4797659", "4795495", "6303935", "4526988",
    "3027756", "4797647", "6303936", "6303937", "4797646",
    "4797651", "4963739", "3015285", "4797661", "4797638",
    "3370151", "6303934", "3990425", "6338564", "4797640",
    "4797645", "4797637", "4797641", "4797644", "4797642",
    "4797655", "4797650", "4797643", "4526989",
    "4797649", "4797648", "5976206", "4797654", "3990426",
    "4797653", "4797656",
]


class OpenAQScraper:
    """Downloads historical hourly PM2.5 data from OpenAQ v3 to CSV files.

    Resolves a curated list of location IDs to their PM2.5 sensor IDs,
    then paginates through the */sensors/{id}/hours* endpoint for each
    station.  Writes one CSV per location to *data/*.
    """

    source_name: str = "openaq"

    def __init__(
        self,
        settings: Settings,
        location_ids: list[str] | None = None,
        date_from: str | None = None,
    ) -> None:
        self._location_ids = location_ids or DEFAULT_HISTORICAL_LOCATION_IDS
        self._date_to = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._date_from = date_from or DEFAULT_DATE_FROM
        self._request_count = 0

        headers = {"Accept": "application/json"}
        if settings.OPENAQ_API_KEY:
            headers["X-API-Key"] = settings.OPENAQ_API_KEY
        self._client = httpx.AsyncClient(base_url=OPENAQ_BASE_URL, headers=headers)

        self._output_dir = Path("data")
        self._output_dir.mkdir(parents=True, exist_ok=True)

    # ── HTTP / rate-limit helpers ──────────────────────────────────────────

    async def _request_with_retry(
        self, url: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """GET with counter-based rate limiting and exponential backoff."""
        if self._request_count > 0 and self._request_count % 240 == 0:
            logger.warning("240 requests reached - sleeping 60 s for rate limit")
            await asyncio.sleep(60)
        self._request_count += 1

        last_error: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = await self._client.get(url, params=params, timeout=30)
                if response.status_code == 429:
                    retry_after = int(
                        response.headers.get("Retry-After", RETRY_DELAYS[attempt - 1])
                    )
                    logger.warning(
                        "429 rate limited (attempt {}/{}). Waiting {} s...",
                        attempt,
                        MAX_RETRIES,
                        retry_after,
                    )
                    await asyncio.sleep(retry_after)
                    continue
                # Don't retry auth errors - they're permanent
                if response.status_code in (401, 403):
                    logger.error(
                        "Auth error {} for {}. Check your OPENAQ_API_KEY.",
                        response.status_code,
                        url,
                    )
                    return None
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.RequestError) as exc:
                last_error = exc
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAYS[attempt - 1]
                    logger.warning(
                        "Request failed (attempt {}/{}): {}. Retry in {} s...",
                        attempt,
                        MAX_RETRIES,
                        exc,
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        "Request failed (attempt {}/{}): {}. Giving up.",
                        attempt,
                        MAX_RETRIES,
                        exc,
                    )
        return None

    # ── Location resolution ───────────────────────────────────────────────

    async def _resolve_location(self, location_id: str) -> tuple[str | None, str | None]:
        """Given a location ID, return ``(sensor_id, name)`` or ``(None, None)``."""
        data = await self._request_with_retry(f"/locations/{location_id}")
        if not data:
            return None, None
        results = data.get("results", [])
        if not results:
            return None, None
        loc = results[0]
        for sensor in loc.get("sensors", []):
            if sensor.get("parameter", {}).get("name") == "pm25":
                return str(sensor["id"]), loc.get("name")
        return None, None

    async def _resolve_all_locations(self) -> list[dict[str, Any]]:
        """Resolve every configured location ID to its sensor + metadata."""
        logger.info("Resolving {} location IDs", len(self._location_ids))
        stations: list[dict[str, Any]] = []

        for loc_id in self._location_ids:
            sensor_id, name = await self._resolve_location(loc_id)
            if sensor_id:
                stations.append(
                    {
                        "station_id": loc_id,
                        "sensor_id": sensor_id,
                        "name": name or f"Location {loc_id}",
                    }
                )
                logger.debug("Resolved {} -> sensor {} ({})", loc_id, sensor_id, name)
            else:
                logger.warning("No PM2.5 sensor for location {}", loc_id)
            await asyncio.sleep(0.5)

        logger.info("Resolved {} stations with PM2.5 sensors", len(stations))
        return stations

    # ── CSV helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _csv_path(location_id: str, location_name: str) -> Path:
        safe = re.sub(r"[^a-zA-Z0-9_\-]", "_", location_name)
        return Path("data") / f"openaq_{location_id}_{safe}.csv"

    @staticmethod
    def _latest_from_csv(csv_path: Path) -> str | None:
        """Read last UTC timestamp from an existing CSV (incremental resume)."""
        if not csv_path.exists():
            return None
        try:
            with open(csv_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                timestamps = [row["utc_timestamp"] for row in reader if row.get("utc_timestamp")]
                if timestamps:
                    return max(timestamps)
        except Exception as exc:
            logger.warning("Could not read dates from {}: {}", csv_path, exc)
        return None

    # ── Main pipeline ──────────────────────────────────────────────────────

    async def run(self) -> int:
        """Resolve stations, fetch hourly data per station, write CSVs."""
        stations = await self._resolve_all_locations()
        if not stations:
            logger.warning("No stations resolved - nothing to fetch")
            return 0

        total = 0

        for station in stations:
            loc_id = station["station_id"]
            sensor_id = station["sensor_id"]
            name = station["name"]

            csv_path = self._csv_path(loc_id, name)
            date_from = self._latest_from_csv(csv_path) or self._date_from

            exists = csv_path.exists()
            logger.info(
                "{} ({}): {} from {}",
                name, loc_id,
                "resuming" if exists else "fresh pull",
                date_from,
            )

            url = f"/sensors/{sensor_id}/hours"
            page = 1
            station_count = 0

            while True:
                params = {
                    "datetime_from": date_from,
                    "datetime_to": self._date_to,
                    "limit": 1000,
                    "page": page,
                }
                data = await self._request_with_retry(url, params=params)
                if not data:
                    break

                results = data.get("results", [])
                if not results:
                    break

                rows = []
                for item in results:
                    utc_str = item.get("period", {}).get("datetimeTo", {}).get("utc")
                    local_str = item.get("period", {}).get("datetimeTo", {}).get("local")
                    value = item.get("value")

                    rows.append(
                        {
                            "location_name": name,
                            "location_id": loc_id,
                            "sensor_id": sensor_id,
                            "utc_timestamp": utc_str,
                            "local_timestamp": local_str,
                            "pm25_value": value,
                            "parameter": "PM2.5",
                            "unit": "µg/m³",
                        }
                    )

                    # Also run through pipeline for data quality logging
                    if utc_str:
                        try:
                            ts = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
                        except (ValueError, AttributeError):
                            ts = datetime.now(timezone.utc)
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=timezone.utc)

                        reading = {
                            "timestamp_utc": ts,
                            "pm25": value,
                        }
                        cleaned = clean_reading(reading)
                        flagged, reason = validate_reading(station, cleaned)
                        if flagged:
                            logger.warning(
                                "Flagged reading at {}: {} (value={})",
                                utc_str, reason, value,
                            )

                mode = "a" if exists else "w"
                with open(csv_path, mode, newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
                    if mode == "w":
                        writer.writeheader()
                        exists = True
                    writer.writerows(rows)

                station_count += len(rows)
                total += len(rows)

                logger.info(
                    "  {} page {} -> {} records (station total: {})",
                    loc_id, page, len(results), station_count,
                )
                page += 1
                await asyncio.sleep(0.5)

            logger.info("{} complete: {} readings", name, station_count)

        logger.info("Done: {} total records across {} stations", total, len(stations))
        return total

    async def close(self) -> None:
        await self._client.aclose()
