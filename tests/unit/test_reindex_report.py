from __future__ import annotations

from datetime import timedelta

from fcp.reindex.report import Savings, format_report


def savings(**kw: int | None) -> Savings:
    base: dict[str, object] = dict(
        window=timedelta(hours=1),
        change_events=100,
        collapsed=10,
        skipped_same_hash=30,
        embedded=58,
        deleted=2,
        selective_tokens=2_900,
        selective_embed_ms=300,
        refresh_runs=4,
        rows_refreshed=1_000,
        corpus_chunks=2_000,
        full_tokens=100_000,
        full_embed_ms=12_000,
    )
    base.update(kw)
    return Savings(**base)  # type: ignore[arg-type]


def test_baselines_and_savings() -> None:
    s = savings()
    assert s.per_event_embeds == 98  # every non-delete change event
    assert s.saving_vs_per_event is not None and round(s.saving_vs_per_event, 4) == round(1 - 58 / 98, 4)
    assert s.per_row_embeds == 1_000
    assert s.saving_vs_per_row is not None and round(s.saving_vs_per_row, 4) == round(1 - 58 / 1_000, 4)
    assert s.full_embeds == 8_000
    assert s.full_tokens_total == 400_000
    assert s.saving_vs_full is not None and round(s.saving_vs_full, 5) == round(1 - 2_900 / 400_000, 5)


def test_report_without_benchmark_says_how_to_get_one() -> None:
    text = format_report(savings(corpus_chunks=None, full_tokens=None, full_embed_ms=None))
    assert "fcp reindex benchmark" in text
    assert "n/a" in text
