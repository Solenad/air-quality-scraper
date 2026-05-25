from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aq_scraper.pipeline.cleaner import clean_reading
from aq_scraper.pipeline.validator import validate_reading
from aq_scraper.storage.models import Reading, Station


class BaseScraper(ABC):
    """Abstract base class for all data source scrapers.

    Each scraper subclass must implement :meth:`fetch_stations` and
    :meth:`fetch_readings`. The :meth:`run` method orchestrates the full
    pipeline: fetch → clean → validate → store. Subclasses can override
    :meth:`run` for custom orchestration if needed.
    """

    source_name: str = ""
    rate_limit_delay: float = 1.0

    @abstractmethod
    async def fetch_stations(self) -> list[dict[str, Any]]:
        """Discover all active monitoring stations for this source.

        Returns
        -------
        list[dict[str, Any]]
            Each dict contains at minimum: station_id, name, latitude,
            longitude, city, and raw_json.
        """
        ...

    @abstractmethod
    async def fetch_readings(self, station: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch the latest readings for a given station.

        Parameters
        ----------
        station : dict[str, Any]
            A station dict previously returned by :meth:`fetch_stations`.

        Returns
        -------
        list[dict[str, Any]]
            Each dict is a single reading conforming to the Reading schema.
        """
        ...

    async def store_station(self, session: AsyncSession, station: dict[str, Any]) -> Station:
        """Upsert a station record and return the ORM object."""
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        stmt = mysql_insert(Station).values(
            station_id=str(station["station_id"]),
            source=self.source_name,
            name=station.get("name"),
            latitude=station.get("latitude"),
            longitude=station.get("longitude"),
            city=station.get("city"),
            province=station.get("province"),
            raw_json=station.get("raw_json"),
        )
        # On duplicate key (station_id, source): update metadata
        upsert = stmt.on_duplicate_key_update(
            name=stmt.inserted.name,
            latitude=stmt.inserted.latitude,
            longitude=stmt.inserted.longitude,
            city=stmt.inserted.city,
            province=stmt.inserted.province,
            raw_json=stmt.inserted.raw_json,
        )
        await session.execute(upsert)

        # Fetch the ORM object to get its PK
        result = await session.execute(
            select(Station).where(
                Station.station_id == str(station["station_id"]),
                Station.source == self.source_name,
            )
        )
        return result.scalar_one()

    async def store_reading(self, session: AsyncSession, reading: dict[str, Any]) -> None:
        """Insert a reading, skipping if it already exists (unique constraint)."""
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        stmt = mysql_insert(Reading).values(**reading)
        stmt = stmt.on_duplicate_key_update(id=stmt.inserted.id)  # no-op on conflict
        await session.execute(stmt)

    async def run(self, session: AsyncSession) -> int:
        """Execute the full scrape pipeline.

        Parameters
        ----------
        session : AsyncSession
            An active SQLAlchemy async session.

        Returns
        -------
        int
            Number of readings stored.
        """
        logger.info("Starting scrape for source: {}", self.source_name)

        # 1. Discover stations
        stations = await self.fetch_stations()
        logger.info("Discovered {} stations from {}", len(stations), self.source_name)

        stored_count = 0
        for station_data in stations:
            # 2. Upsert station in DB
            station_obj = await self.store_station(session, station_data)

            # 3. Fetch readings for this station
            readings = await self.fetch_readings(station_data)

            for raw_reading in readings:
                # 4. Clean
                cleaned = clean_reading(raw_reading)

                # 5. Validate
                flagged, reason = validate_reading(station_data, cleaned)

                # 6. Prepare Reading dict
                reading_record = {
                    "station_id": station_obj.id,
                    "source": self.source_name,
                    "timestamp_utc": cleaned["timestamp_utc"],
                    "pm25": cleaned.get("pm25"),
                    "pm10": cleaned.get("pm10"),
                    "aqi": cleaned.get("aqi"),
                    "temperature": cleaned.get("temperature"),
                    "humidity": cleaned.get("humidity"),
                    "wind_speed": cleaned.get("wind_speed"),
                    "wind_direction": cleaned.get("wind_direction"),
                    "is_flagged": flagged,
                    "flag_reason": reason,
                    "raw_json": station_data.get("raw_json"),
                }

                # 7. Store
                await self.store_reading(session, reading_record)
                stored_count += 1

        logger.info(
            "Scrape complete for {}: {} readings stored",
            self.source_name,
            stored_count,
        )
        return stored_count
