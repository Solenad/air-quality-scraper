from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

PH_LAT_MIN = 4.5
PH_LAT_MAX = 21.5
PH_LON_MIN = 116.0
PH_LON_MAX = 127.0
PM25_MIN = 0.0
PM25_MAX = 1000.0


def validate_reading(station: dict[str, Any], reading: dict[str, Any]) -> tuple[bool, str | None]:
    """Check a reading against known data-quality rules.

    Parameters
    ----------
    station : dict
        Station metadata including ``latitude`` and ``longitude``.
    reading : dict
        Cleaned reading dict from ``clean_reading``.

    Returns
    -------
    tuple[bool, str | None]
        ``(is_flagged, flag_reason)``. If valid, ``(False, None)``.
    """
    # 1. PM2.5 physical range
    pm25 = reading.get("pm25")
    if pm25 is not None:
        try:
            val = float(pm25)
            if val < PM25_MIN or val > PM25_MAX:
                return True, "pm25_out_of_range"
        except (TypeError, ValueError):
            return True, "pm25_out_of_range"

    # 2. Future timestamp
    ts = reading.get("timestamp_utc")
    if ts is not None and isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts > datetime.now(timezone.utc):
            return True, "future_timestamp"

    # 3. Coordinates outside PH bounding box
    lat = station.get("latitude")
    lon = station.get("longitude")
    if lat is not None and lon is not None:
        try:
            lat_f, lon_f = float(lat), float(lon)
            if lat_f < PH_LAT_MIN or lat_f > PH_LAT_MAX or lon_f < PH_LON_MIN or lon_f > PH_LON_MAX:
                return True, "coordinates_outside_ph_bbox"
        except (TypeError, ValueError):
            return True, "coordinates_outside_ph_bbox"

    return False, None
