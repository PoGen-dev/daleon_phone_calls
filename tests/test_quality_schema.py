from __future__ import annotations

from app.common.models import QualityResult


def test_quality_result_accepts_valid_payload() -> None:
    payload = {
        "score": 88,
        "summary": "Хороший разговор с понятным следующим шагом.",
        "positives": ["Вежливо"],
        "negatives": ["Мало выявления потребностей"],
        "recommendations": ["Задавать больше уточняющих вопросов"],
        "criteria": {
            "greeting": 100,
            "needs_discovery": 70,
            "clarity": 90,
            "empathy": 90,
            "resolution": 85,
            "compliance": 95,
        },
    }
    result = QualityResult.model_validate(payload)
    assert result.score == 88
