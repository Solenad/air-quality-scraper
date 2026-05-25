from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import httpx
from loguru import logger

from aq_scraper.config.settings import Settings
from aq_scraper.scrapers.base import BaseScraper

OPENAQ_BASE_URL = "https://api.openaq.org/v3"
PH_BBOX = "116,4.5,127,21.5"
PM25_PARAMETER_ID = 2
MAX_RETRIES = 4
RETRY_DELAYS = [1, 2, 4, 8]


class OpenAQScraper(BaseScraper):
    """Scraper for the OpenAQ v3 REST API.

    Discovers active PM2.5 stations in the Philippines bounding box and
    fetches their latest readings in a single paginated request.
    """

    source_name: str = "openaq"
    rate_limit_delay: float = 0.0  # OpenAQ generous rate limits

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        headers = {"Accept": "application/json"}
        if settings.OPENAQ_API_KEY:
            headers["X-API-Key"] = settings.OPENAQ_API_KEY
        self._client = httpx.AsyncClient(base_url=OPENAQ_BASE_URL, headers=headers)

    async def _request_with_retry(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a GET request with exponential backoff retry."""
        last_error: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = await self._client.get(url, params=params, timeout=30)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", RETRY_DELAYS[attempt - 1]))
                    logger.warning("Rate limited (attempt {}/{}). Waiting {}s...", attempt, MAX_RETRIES, retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.RequestError) as exc:
                last_error = exc
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAYS[attempt - 1]
                    logger.warning("Request failed (attempt {}/{}): {}. Retrying in {}s...", attempt, MAX_RETRIES, exc, delay)
                    await asyncio.sleep(delay)
                else:
                    logger.error("Request failed (attempt {}/{}): {}. Giving up.", attempt, MAX_RETRIES, exc)
        raise RuntimeError(f"Request to {url} failed after {MAX_RETRIES} retries") from last_error

    async def fetch_stations(self) -> list[dict[str, Any]]:
        """Discover active PM2.5 stations in the Philippines bounding box.

        Paginates through all pages of the /v3/locations endpoint.
        """
        logger.info("Fetching stations from OpenAQ (bbox={})", PH_BBOX)
        all_results: list[dict[str, Any]] = []
        page = 1

        while True:
            params: dict[str, Any] = {
                "bbox": PH_BBOX,
                "parameters_id": PM25_PARAMETER_ID,
                "limit": 1000,
                "page": page,
            }
            data = await self._request_with_retry("/locations", params=params)
            results = data.get("results", [])
            all_results.extend(results)

            meta = data.get("meta", {})
            found = meta.get("found", 0)
            limit = meta.get("limit", 1000)

            if len(all_results) >= found or len(results) == 0:
                break
            page += 1

        logger.info("Found {} stations from OpenAQ", len(all_results))

        # Map to standard station dict format
        stations = []
        for loc in all_results:
            coords = loc.get("coordinates", {})
            stations.append(
                {
                    "station_id": str(loc["id"]),
                    "name": loc.get("name"),
                    "latitude": coords.get("latitude"),
                    "longitude": coords.get("longitude"),
                    "city": loc.get("locality") or loc.get("timezone"),
                    "province": None,
                    "raw_json": loc,
                }
            )
        return stations

    async def fetch_readings(self, station: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract the latest PM2.5 reading from the location's parameters array.

        The /v3/locations endpoint already includes latest values in the
        response, so this method extracts from the cached raw_json rather
        than making an additional API call.
        """
        raw: dict[str, Any] = station.get("raw_json", {})
        parameters = raw.get("parameters", [])

        # Find the PM2.5 parameter entry (parameter.id == 2)
        pm25_entry: dict[str, Any] | None = None
        for param in parameters:
            param_info = param.get("parameter", {})
            if isinstance(param_info, dict) and param_info.get("id") == PM25_PARAMETER_ID:
                pm25_entry = param
                break
            if isinstance(param_info, (int, str)) and int(param_info) == PM25_PARAMETER_ID:
                pm25_entry = param
                break

        if pm25_entry is None:
            logger.debug("No PM2.5 parameter for station {}", station["station_id"])
            return []

        # Parse timestamp
        last_updated = pm25_entry.get("lastUpdated")
        if last_updated:
            try:
                ts = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                ts = datetime.now(timezone.utc)
        else:
            ts = datetime.now(timezone.utc)

        # Ensure timezone-aware
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        reading: dict[str, Any] = {
            "timestamp_utc": ts,
            "pm25": pm25_entry.get("lastValue"),
            "pm10": None,
            "aqi": None,
            "temperature": None,
            "humidity": None,
            "wind_speed": None,
            "wind_direction": None,
        }
        return [reading]

    async def close(self) -> None:
        await self._client.aclose()
