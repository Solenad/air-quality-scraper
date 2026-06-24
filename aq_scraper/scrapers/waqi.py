from __future__ import annotations

import asyncio
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from loguru import logger

from aq_scraper.config.settings import Settings
from aq_scraper.pipeline.cleaner import clean_reading
from aq_scraper.pipeline.validator import validate_reading

WAQI_BASE_URL = "https://api.waqi.info"
MAPQ_URL = "https://mapq.waqi.info/mapq2/bounds"
MAPQ_PUBLIC_KEY = "_2Y2EnVR5mHV0fHScOSBRWXmpNbEE9LRkaFkYdZQ=="

MAX_RETRIES = 4
RETRY_DELAYS = [1, 2, 4, 8]

MANILA_BBOX = (120.85, 14.20, 121.20, 14.85)

PM25_BREAKPOINTS = [
    (0,    50,    0.0,   12.0),
    (51,   100,   12.1,  35.4),
    (101,  150,   35.5,  55.4),
    (151,  200,   55.5,  150.4),
    (201,  300,   150.5, 250.4),
    (301,  400,   250.5, 350.4),
    (401,  500,   350.5, 500.4),
]

CSV_FIELDNAMES = [
    "station_id", "station_name", "latitude", "longitude",
    "utc_timestamp", "local_timestamp", "timezone",
    "aqi", "dominentpol",
    "pm25_aqi", "pm25_ugm3",
    "temperature", "humidity", "wind_speed", "pressure",
    "co", "no2", "o3", "so2",
    "raw_json",
]


KNOWN_OFFICIAL_STATIONS: list[dict[str, Any]] = [
    {"uid": "14893", "name": "Manila US Embassy", "latitude": 14.57711, "longitude": 120.9778},
    {"uid": "8259", "name": "Manila Center", "latitude": 14.5995124, "longitude": 120.9842195},
    {"uid": "9567", "name": "Manila Quezon City", "latitude": 14.6760413, "longitude": 121.0437003},
    {"uid": "9569", "name": "City Hall, Meycauayan City", "latitude": 14.744046, "longitude": 120.9613027},
    {"uid": "11368", "name": "City Station, Santa Rosa", "latitude": 14.307201385498, "longitude": 121.11041259766},
    {"uid": "11938", "name": "Biñan City", "latitude": 14.3036345, "longitude": 121.0781493},
]


class WAQIScraper:
    """Fetches real-time AQI snapshots from WAQI (aqicn.org).

    Station discovery uses the public mapq2/bounds API (no auth required).
    Data fetching uses the personal WAQI_API_KEY via /feed/@station_id/.
    Each station gets its own CSV file under data/waqi/.
    """

    source_name: str = "waqi"

    def __init__(self, settings: Settings) -> None:
        self._api_key = settings.WAQI_API_KEY
        if not self._api_key:
            logger.error("WAQI_API_KEY is required. Set it in .env or environment.")
            raise ValueError("WAQI_API_KEY is required")

        self._client = httpx.AsyncClient(
            base_url=WAQI_BASE_URL,
            params={"token": self._api_key},
            headers={"Accept": "application/json"},
        )

        self._output_dir = Path("data") / "waqi"
        self._output_dir.mkdir(parents=True, exist_ok=True)

    async def _request_with_retry(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = await self._client.get(url, params=params, timeout=30)
                if response.status_code == 429:
                    retry_after = int(
                        response.headers.get("Retry-After", RETRY_DELAYS[attempt - 1])
                    )
                    logger.warning(
                        "429 rate limited (attempt {}/{}). Waiting {} s...",
                        attempt, MAX_RETRIES, retry_after,
                    )
                    await asyncio.sleep(retry_after)
                    continue
                if response.status_code in (401, 403):
                    logger.error(
                        "Auth error {} for {}. Check your WAQI_API_KEY.",
                        response.status_code, url,
                    )
                    return None
                response.raise_for_status()
                return response.json()
            except httpx.ConnectError as exc:
                logger.error("Connection failed for {}: {}", url, exc)
                return None
            except (httpx.TimeoutException, httpx.HTTPStatusError) as exc:
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAYS[attempt - 1]
                    logger.warning(
                        "Request failed (attempt {}/{}): {}. Retry in {} s...",
                        attempt, MAX_RETRIES, exc, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        "Request failed (attempt {}/{}): {}. Giving up.",
                        attempt, MAX_RETRIES, exc,
                    )
        return None

    async def _discover_stations(self) -> list[dict[str, Any]]:
        """Discover Manila stations via the public mapq2/bounds API."""
        logger.info("Discovering stations in Manila bbox={}", MANILA_BBOX)

        payload = {
            "key": MAPQ_PUBLIC_KEY,
            "bounds": "{:.2f},{:.2f},{:.2f},{:.2f}".format(*MANILA_BBOX),
            "zoom": "12",
            "inc": "placeholders",
            "viewer": "webgl",
            "country": "",
        }

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    MAPQ_URL,
                    data=payload,
                    timeout=30,
                )
                resp.raise_for_status()
                body = resp.json()
        except Exception as exc:
            logger.error("Map discovery failed: {}", exc)
            return []

        raw_stations = body.get("data", [])
        if not raw_stations:
            logger.warning("No stations returned from map discovery")
            return []

        logger.info("Raw map returned {} station(s)", len(raw_stations))

        map_out = self._output_dir / "map_discovery.json"
        with open(map_out, "w", encoding="utf-8") as f:
            json.dump(body, f, indent=2, ensure_ascii=False)
        logger.info("Saved raw map discovery to {}", map_out)

        stations = []
        stale = 0
        for s in raw_stations:
            aqi_str = s.get("aqi", "-")
            if aqi_str == "-" or aqi_str == "":
                stale += 1
                continue
            try:
                aqi_val = int(aqi_str)
            except (ValueError, TypeError):
                stale += 1
                continue
            if aqi_val <= 0:
                stale += 1
                continue

            geo = s.get("geo", [])
            stations.append({
                "uid": s["idx"],
                "name": s.get("name", f"Station {s['idx']}"),
                "latitude": float(geo[0]) if len(geo) > 0 else None,
                "longitude": float(geo[1]) if len(geo) > 1 else None,
                "aqi": aqi_val,
            })

        if stale:
            logger.info("Filtered out {} station(s) with no current AQI", stale)
        else:
            logger.info("All {} station(s) have current AQI data", len(stations))

        logger.info("Discovered {} active station(s)", len(stations))
        return stations

    async def _fetch_station_data(self, station: dict[str, Any]) -> dict[str, Any] | None:
        uid = station["uid"]
        data = await self._request_with_retry(f"/feed/@{uid}/")
        if not data or data.get("status") != "ok":
            logger.warning("No data for station uid={} ({})", uid, station["name"])
            return None
        return data

    async def _fetch_all_stations(self) -> list[tuple[dict[str, Any], dict[str, Any]]]:
        discovered = await self._discover_stations()
        known = [dict(s) for s in KNOWN_OFFICIAL_STATIONS]

        seen = {s["uid"] for s in discovered}
        for s in known:
            if s["uid"] not in seen:
                discovered.append(s)
                seen.add(s["uid"])

        if not discovered:
            logger.warning("No stations discovered — nothing to fetch")
            return []

        logger.info("Fetching full data for {} station(s)", len(discovered))
        results: list[tuple[dict[str, Any], dict[str, Any]]] = []

        for i, station in enumerate(discovered):
            logger.info(
                "[{}/{}] Fetching station uid={} ({})",
                i + 1, len(discovered), station["uid"], station["name"],
            )
            data = await self._fetch_station_data(station)
            if data:
                results.append((station, data))
            await asyncio.sleep(0.35)

        if results:
            logger.info("Fetched data for {} station(s)", len(results))
        else:
            logger.warning("No station data returned from feed API")

        return results

    @staticmethod
    def _pm25_aqi_to_ugm3(aqivalue: Any) -> float | None:
        if aqivalue is None:
            return None
        try:
            aqi = float(aqivalue)
        except (TypeError, ValueError):
            return None
        if aqi <= 0:
            return None
        for aqi_low, aqi_high, conc_low, conc_high in PM25_BREAKPOINTS:
            if aqi_low <= aqi <= aqi_high:
                if aqi_low == aqi_high:
                    return conc_high
                return ((aqi - aqi_low) / (aqi_high - aqi_low)) * (conc_high - conc_low) + conc_low
        last = PM25_BREAKPOINTS[-1]
        ratio = (aqi - last[1]) / (last[1] - last[0])
        return last[3] + ratio * (last[3] - last[2])

    def _parse_snapshot(self, data: dict[str, Any], station: dict[str, Any]) -> dict[str, Any] | None:
        d = data.get("data", {})
        if not d:
            return None

        time_info = d.get("time", {})
        city_info = d.get("city", {})
        geo = city_info.get("geo", [])
        iaqi = d.get("iaqi", {})

        row: dict[str, Any] = {
            "station_id": d.get("idx"),
            "station_name": city_info.get("name", station.get("name", "")),
            "latitude": float(geo[0]) if len(geo) > 0 else station.get("latitude"),
            "longitude": float(geo[1]) if len(geo) > 1 else station.get("longitude"),
            "utc_timestamp": time_info.get("iso", ""),
            "local_timestamp": time_info.get("s", ""),
            "timezone": time_info.get("tz", ""),
            "aqi": d.get("aqi"),
            "dominentpol": d.get("dominentpol", ""),
            "pm25_aqi": None,
            "pm25_ugm3": None,
            "temperature": None,
            "humidity": None,
            "wind_speed": None,
            "pressure": None,
            "co": None,
            "no2": None,
            "o3": None,
            "so2": None,
        }

        pm25 = iaqi.get("pm25", {})
        if pm25 and pm25.get("v") is not None:
            row["pm25_aqi"] = pm25["v"]
            row["pm25_ugm3"] = self._pm25_aqi_to_ugm3(pm25["v"])

        field_map = {"t": "temperature", "h": "humidity", "w": "wind_speed", "p": "pressure"}
        for short_key, long_key in field_map.items():
            item = iaqi.get(short_key, {})
            if item and item.get("v") is not None:
                row[long_key] = item["v"]

        for key in ("co", "no2", "o3", "so2"):
            item = iaqi.get(key, {})
            if item and item.get("v") is not None:
                row[key] = item["v"]

        row["raw_json"] = json.dumps(d)
        return row

    def _csv_path(self, station: dict[str, Any]) -> Path:
        safe_name = re.sub(r"[^a-zA-Z0-9_\-]", "_", station.get("name", str(station["uid"])))
        sanitised = re.sub(r"_+", "_", safe_name).strip("_")
        return self._output_dir / f"waqi_{station['uid']}_{sanitised}.csv"

    def _write_csv(self, station: dict[str, Any], row: dict[str, Any]) -> None:
        csv_path = self._csv_path(station)
        exists = csv_path.exists()
        mode = "a" if exists else "w"
        with open(csv_path, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            if mode == "w":
                writer.writeheader()
            writer.writerow(row)

    async def run(self) -> int:
        logger.info("Starting WAQI scraper (Manila)")

        stations_with_data = await self._fetch_all_stations()
        if not stations_with_data:
            logger.warning("No stations discovered — nothing to fetch")
            return 0

        total = 0
        for station, data in stations_with_data:
            logger.info("Processing station {} ({})", station["uid"], station["name"])

            row = self._parse_snapshot(data, station)
            if not row:
                logger.warning("Could not parse data for station {}", station["uid"])
                continue

            try:
                ts_str = row.get("utc_timestamp", "")
                if ts_str:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                else:
                    ts = datetime.now(timezone.utc)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)

                reading = {
                    "timestamp_utc": ts,
                    "pm25": row.get("pm25_ugm3"),
                    "aqi": row.get("aqi"),
                    "temperature": row.get("temperature"),
                    "humidity": row.get("humidity"),
                    "wind_speed": row.get("wind_speed"),
                }
                cleaned = clean_reading(reading)
                station_meta = {
                    "latitude": row.get("latitude"),
                    "longitude": row.get("longitude"),
                }
                flagged, reason = validate_reading(station_meta, cleaned)
                if flagged:
                    logger.warning("Flagged reading at {}: {}", ts_str, reason)
            except Exception as exc:
                logger.warning("Pipeline error for station {}: {}", station["uid"], exc)

            self._write_csv(station, row)
            total += 1
            logger.info("Written: uid={}, name={}", station["uid"], station["name"])

        logger.info("WAQI scraper complete: {} station(s)", total)
        return total

    async def close(self) -> None:
        await self._client.aclose()
