from __future__ import annotations

import csv
from datetime import date, datetime
from pathlib import Path

from loguru import logger
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

# Columns to export in the CSV (JOINed from stations + readings)
EXPORT_COLUMNS = [
    "station_id",
    "source",
    "station_name",
    "latitude",
    "longitude",
    "city",
    "timestamp_utc",
    "pm25",
    "pm10",
    "aqi",
    "temperature",
    "humidity",
    "wind_speed",
    "wind_direction",
    "is_flagged",
    "flag_reason",
    "raw_json",
]

EXPORT_QUERY = text("""
    SELECT
        s.station_id,
        r.source,
        s.name              AS station_name,
        s.latitude,
        s.longitude,
        s.city,
        r.timestamp_utc,
        r.pm25,
        r.pm10,
        r.aqi,
        r.temperature,
        r.humidity,
        r.wind_speed,
        r.wind_direction,
        r.is_flagged,
        r.flag_reason,
        r.raw_json
    FROM readings r
    JOIN stations s ON r.station_id = s.id
    WHERE r.source = :source
    ORDER BY r.timestamp_utc DESC
""")


async def export_csv(source_name: str, session: AsyncSession) -> Path | None:
    """Export readings from the database to a date-suffixed CSV file.

    Parameters
    ----------
    source_name : str
        Source identifier (e.g. ``"openaq"``).
    session : AsyncSession
        Active database session.

    Returns
    -------
    Path or None
        Path to the created CSV file, or ``None`` if no data was exported.
    """
    from sqlalchemy import text as sql_text

    # Determine output path
    output_dir = Path("data")
    output_dir.mkdir(parents=True, exist_ok=True)

    today = date.today().isoformat()
    output_path = output_dir / f"{source_name}_readings_{today}.csv"

    # Fetch rows from the JOINed query
    result = await session.execute(sql_text(EXPORT_QUERY.text), {"source": source_name})
    rows = result.fetchall()

    if not rows:
        logger.warning("No readings found for source '{}' — skipping CSV export", source_name)
        return None

    # Write CSV
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(EXPORT_COLUMNS)
        for row in rows:
            writer.writerow(list(row))

    logger.info("Exported {} readings to {}", len(rows), output_path)
    return output_path
