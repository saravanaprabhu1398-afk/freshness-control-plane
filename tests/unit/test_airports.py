from __future__ import annotations

import pytest

from fcp.common.airports import BY_IATA, TRACKED_AIRPORTS, covering_bbox, haversine_km, nearest_tracked


def test_haversine_jfk_lax_matches_published_great_circle_distance() -> None:
    jfk, lax = BY_IATA["JFK"], BY_IATA["LAX"]
    # Published great-circle distance JFK-LAX is ~3,983 km (2,475 mi).
    assert haversine_km(jfk.latitude, jfk.longitude, lax.latitude, lax.longitude) == pytest.approx(
        3983, abs=10
    )


def test_nearest_tracked_inside_and_outside_radius() -> None:
    ord_ = BY_IATA["ORD"]
    assert nearest_tracked(ord_.latitude + 0.2, ord_.longitude, radius_km=80) == ord_
    assert nearest_tracked(ord_.latitude + 1.0, ord_.longitude, radius_km=80) is None  # ~111 km away


def test_covering_bbox_contains_every_airport_with_padding() -> None:
    radius = 80.0
    box = covering_bbox(radius)
    for ap in TRACKED_AIRPORTS:
        assert box.lamin < ap.latitude - 0.7 and ap.latitude + 0.7 < box.lamax
        assert box.lomin < ap.longitude - 0.9 and ap.longitude + 0.9 < box.lomax
