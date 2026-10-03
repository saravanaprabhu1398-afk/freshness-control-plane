from __future__ import annotations

import pytest
import yaml

from fcp.common.settings import get_settings
from fcp.freshness.contracts import parse
from fcp.ingestion.ourairports import parse as parse_airports


def test_repo_contracts_file_is_valid_and_covers_every_registry_contract() -> None:
    version, contracts = parse(yaml.safe_load(get_settings().contracts_file.read_text()))
    names = {c.name for c in contracts}
    # Every contract name the loaders write to freshness.registry must be declared.
    assert {"flight_status", "fare", "airport_reference", "on_time_performance", "fare_baseline"} <= names
    assert version >= 1


@pytest.mark.parametrize(("field", "value"), [("measure", "wall_clock"), ("on_breach", "explode")])
def test_contract_validation_rejects_bad_values(field: str, value: str) -> None:
    doc = {
        "version": 1,
        "contracts": [
            {"name": "x", "measure": "data_as_of", "max_age": "1 day", "on_breach": "flag", field: value}
        ],
    }
    with pytest.raises(ValueError, match=field):
        parse(doc)


def test_contract_validation_rejects_duplicates() -> None:
    c = {"name": "x", "measure": "data_as_of", "max_age": "1 day", "on_breach": "flag"}
    with pytest.raises(ValueError, match="duplicate"):
        parse({"version": 1, "contracts": [c, dict(c)]})


def test_ourairports_parse_keeps_us_large_medium_with_iata() -> None:
    header = (
        "id,ident,type,name,latitude_deg,longitude_deg,elevation_ft,continent,iso_country,iso_region,"
        "municipality,scheduled_service,gps_code,iata_code,local_code,home_link,wikipedia_link,keywords"
    )
    rows = [
        '1,KORD,large_airport,"Chicago O\'Hare",41.97,-87.90,680,NA,US,US-IL,Chicago,yes,KORD,ORD,ORD,,,',
        "2,KXYZ,small_airport,Tiny,40,-80,100,NA,US,US-PA,Town,no,,XYZ,,,,",
        "3,CYYZ,large_airport,Toronto,43.6,-79.6,569,NA,CA,CA-ON,Toronto,yes,CYYZ,YYZ,,,,",
        "4,KABC,medium_airport,No IATA,40,-80,,NA,US,US-PA,Town,no,,,,,,",
    ]
    parsed = parse_airports("\n".join([header, *rows]))
    assert [r["ident"] for r in parsed] == ["KORD"]
    assert parsed[0]["elevation_ft"] == 680
