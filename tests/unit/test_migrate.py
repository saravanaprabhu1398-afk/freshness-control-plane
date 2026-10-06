from __future__ import annotations

from pathlib import Path

import pytest

from fcp.common.migrate import MIGRATIONS_DIR, MigrationError, discover


def test_repo_migrations_are_ordered_and_unique() -> None:
    found = discover(MIGRATIONS_DIR)
    versions = [m.version for m in found]
    assert versions == sorted(versions)
    assert versions[0] == "0001"
    assert len(set(versions)) == len(versions)


def test_rejects_bad_names_and_duplicates(tmp_path: Path) -> None:
    (tmp_path / "1_x.sql").write_text("select 1")
    (tmp_path / "1_y.sql").write_text("select 1")
    with pytest.raises(MigrationError, match="duplicate"):
        discover(tmp_path)
    (tmp_path / "abc.sql").write_text("select 1")
    with pytest.raises(MigrationError, match="bad migration file name"):
        discover(tmp_path)
