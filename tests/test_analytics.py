from __future__ import annotations

from datetime import UTC, datetime

from app.common.analytics import (
    AnalyticsFilters,
    _build_where,
    _comparison,
    _enrich_summary,
)


def test_build_where_uses_bound_parameters() -> None:
    filters = AnalyticsFilters(
        date_from=datetime(2026, 8, 1, tzinfo=UTC),
        date_to=datetime(2026, 9, 1, tzinfo=UTC),
        account="second",
        manager="Manager",
        score_min=50,
        has_transcription=True,
        search="7921' OR 1=1 --",
    )
    where, values = _build_where(filters)

    assert "e.started_at >= $1" in where
    assert "e.started_at < $2" in where
    assert "e.mango_account = $3" in where
    assert "e.manager_name = $4" in where
    assert "e.score >= $5" in where
    assert "e.transcript IS NOT NULL" in where
    assert "ILIKE $6" in where
    assert "OR 1=1" not in where
    assert values[-1] == "%7921' OR 1=1 --%"


def test_build_where_handles_negative_boolean_filters() -> None:
    where, values = _build_where(
        AnalyticsFilters(has_transcription=False, has_quality=False)
    )
    assert "e.transcript IS NULL" in where
    assert "e.score IS NULL" in where
    assert values == []


def test_enrich_summary_computes_pipeline_rates() -> None:
    result = _enrich_summary(
        {
            "total_calls": 100,
            "transcribed_calls": 95,
            "classified_calls": 90,
            "analyzed_calls": 80,
            "notified_calls": 75,
            "completed_deal_calls": 18,
            "appointment_calls": 27,
            "critical_calls": 8,
        }
    )
    assert result["transcription_coverage"] == 95.0
    assert result["analysis_coverage"] == 80.0
    assert result["notification_coverage"] == 75.0
    assert result["deal_rate"] == 20.0
    assert result["appointment_rate"] == 30.0
    assert result["critical_rate"] == 10.0


def test_comparison_reports_percent_and_point_changes() -> None:
    current = {
        "total_calls": 120,
        "avg_score": 81.0,
        "critical_calls": 6,
        "completed_deal_calls": 24,
        "deal_rate": 20.0,
        "analysis_coverage": 90.0,
    }
    previous = {
        "total_calls": 100,
        "avg_score": 78.5,
        "critical_calls": 10,
        "completed_deal_calls": 20,
        "deal_rate": 18.0,
        "analysis_coverage": 85.0,
    }
    delta = _comparison(current, previous)["delta"]
    assert delta["total_calls_pct"] == 20.0
    assert delta["avg_score"] == 2.5
    assert delta["critical_calls_pct"] == -40.0
    assert delta["deal_rate_pp"] == 2.0
    assert delta["analysis_coverage_pp"] == 5.0
