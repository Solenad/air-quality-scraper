from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def clean_reading(reading: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw reading dict.

    - Ensures ``timestamp_utc`` is a timezone-aware :class:`datetime`.
    - Strips whitespace from all string values.
    - Parses numeric coordinates as floats.
    - Drops unrecognised keys.

    Parameters
    ----------
    reading : dict
        Raw reading dict from a scraper's ``fetch_readings``.

    Returns
    -------
    dict
        Cleaned dict safe for validation and storage.
    """
    cleaned: dict[str, Any] = {}

    for key, value in reading.items():
        # Strip whitespace from strings
        if isinstance(value, str):
            value = value.strip()
        cleaned[key] = value

    # Normalise timestamp
    ts = cleaned.get("timestamp_utc")
    if ts is not None:
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                ts = datetime.now(timezone.utc)
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        cleaned["timestamp_utc"] = ts
    else:
        cleaned["timestamp_utc"] = datetime.now(timezone.utc)

    # Parse numeric fields
    for num_key in ("pm25", "pm10", "temperature", "humidity", "wind_speed", "wind_direction"):
        val = cleaned.get(num_key)
        if val is not None:
            try:
                cleaned[num_key] = float(val)
            except (TypeError, ValueError):
                cleaned[num_key] = None

    # Parse integer fields
    aqi = cleaned.get("aqi")
    if aqi is not None:
        try:
            cleaned["aqi"] = int(float(aqi))
        except (TypeError, ValueError):
            cleaned["aqi"] = None

    return cleaned
