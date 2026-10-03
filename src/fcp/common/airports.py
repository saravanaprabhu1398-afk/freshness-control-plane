"""The airports this project tracks (PRD §4: US hubs, because BTS covers US domestic flights).

Coordinates are the aerodrome reference points from OurAirports. They are duplicated here
so the OpenSky poller does not depend on the reference load having run first.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Airport:
    iata: str
    icao: str
    name: str
    latitude: float
    longitude: float


TRACKED_AIRPORTS: tuple[Airport, ...] = (
    Airport("ATL", "KATL", "Hartsfield-Jackson Atlanta International", 33.6367, -84.4281),
    Airport("ORD", "KORD", "Chicago O'Hare International", 41.9786, -87.9048),
    Airport("DFW", "KDFW", "Dallas/Fort Worth International", 32.8968, -97.0380),
    Airport("DEN", "KDEN", "Denver International", 39.8617, -104.6731),
    Airport("LAX", "KLAX", "Los Angeles International", 33.9425, -118.4081),
    Airport("JFK", "KJFK", "John F. Kennedy International", 40.6398, -73.7789),
    Airport("SFO", "KSFO", "San Francisco International", 37.6190, -122.3750),
    Airport("SEA", "KSEA", "Seattle-Tacoma International", 47.4490, -122.3093),
)

BY_IATA: dict[str, Airport] = {a.iata: a for a in TRACKED_AIRPORTS}
BY_ICAO: dict[str, Airport] = {a.icao: a for a in TRACKED_AIRPORTS}
TRACKED_IATA: frozenset[str] = frozenset(BY_IATA)

EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi, dlmb = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def nearest_tracked(lat: float, lon: float, radius_km: float) -> Airport | None:
    """Return the closest tracked airport within `radius_km`, or None."""
    best: tuple[float, Airport] | None = None
    for ap in TRACKED_AIRPORTS:
        d = haversine_km(lat, lon, ap.latitude, ap.longitude)
        if d <= radius_km and (best is None or d < best[0]):
            best = (d, ap)
    return best[1] if best else None


@dataclass(frozen=True, slots=True)
class BBox:
    lamin: float
    lomin: float
    lamax: float
    lomax: float

    @property
    def area_sq_deg(self) -> float:
        return (self.lamax - self.lamin) * (self.lomax - self.lomin)


def covering_bbox(radius_km: float) -> BBox:
    """Smallest lat/lon box that covers every tracked airport plus `radius_km` around it."""
    pad_lat = radius_km / 111.0
    lats = [a.latitude for a in TRACKED_AIRPORTS]
    lons = [a.longitude for a in TRACKED_AIRPORTS]
    # Longitude degrees shrink with latitude; pad using the highest latitude (worst case).
    pad_lon = radius_km / (111.0 * math.cos(math.radians(max(lats) + pad_lat)))
    return BBox(
        lamin=round(min(lats) - pad_lat, 2),
        lomin=round(min(lons) - pad_lon, 2),
        lamax=round(max(lats) + pad_lat, 2),
        lomax=round(max(lons) + pad_lon, 2),
    )
