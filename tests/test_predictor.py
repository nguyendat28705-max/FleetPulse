"""Unit tests cho Predictor Interface, RuleBasedPredictor, MLPredictor và cơ chế Fallback."""

import math
from pathlib import Path
import joblib
import numpy as np
import pytest
from sklearn.linear_model import LinearRegression

from fp_algo.contract import FEATURE_ORDER
from fp_algo.predictor import (
    BasePredictor,
    MLPredictor,
    RuleBasedPredictor,
    create_predictor,
)


def test_rule_based_predictor() -> None:
    pred = RuleBasedPredictor()

    # Mẫu bình thường
    healthy_feat = {
        "tp2_mean": 1.415,
        "tp2_std": 3.28,
        "loaded_ratio": 0.42,
        "rise_rate": 0.07,
        "cycle_len_s": 38.0,
    }
    hi_val = pred.predict_health(healthy_feat)
    assert 0.0 <= hi_val < 3.0

    rul_res = pred.predict_rul(healthy_feat, hi_val, t=100.0)
    assert "rul_hours" in rul_res
    assert rul_res["rul_hours"] > 0
    assert rul_res["rul_low"] <= rul_res["rul_hours"] <= rul_res["rul_high"]
    assert rul_res["confidence"] == 0.95

    # Test silence prediction
    silence_res = pred.predict_silence(t=150.0)
    assert "rul_hours" in silence_res


def test_ml_predictor_fallback_when_model_missing(tmp_path: Path) -> None:
    """Khi file mô hình không tồn tại, MLPredictor tự động fallback về Baseline an toàn."""
    fake_path = tmp_path / "non_existent_model.joblib"
    pred = MLPredictor(model_path=fake_path, fallback_to_baseline=True)

    assert pred.model_loaded is False

    feat = {
        "tp2_mean": 1.415,
        "tp2_std": 3.28,
        "loaded_ratio": 0.42,
        "rise_rate": 0.07,
        "cycle_len_s": 38.0,
    }

    # Phải gọi được mà không văng exception
    hi_val = pred.predict_health(feat)
    assert 0.0 <= hi_val < 5.0
    assert pred.fallback_count == 1

    rul_res = pred.predict_rul(feat, hi_val, t=200.0)
    assert rul_res["rul_hours"] > 0
    assert pred.fallback_count == 2


def test_ml_predictor_with_trained_model(tmp_path: Path) -> None:
    """Kiểm tra MLPredictor nạp và suy luận thành công với một mô hình scikit-learn thật."""
    # Tạo mô hình scikit-learn nhỏ
    X_train = np.random.randn(50, len(FEATURE_ORDER))
    y_hi = np.clip(np.abs(X_train[:, 0] * 1.5), 0.5, 8.0)
    y_rul = np.clip(100.0 - y_hi * 10.0, 5.0, 150.0)

    hi_reg = LinearRegression().fit(X_train, y_hi)
    rul_reg = LinearRegression().fit(X_train, y_rul)

    bundle = {
        "hi_model": hi_reg,
        "rul_model": rul_reg,
        "feature_order": FEATURE_ORDER,
        "contract_version": "1.1.0",
    }
    model_file = tmp_path / "test_model.joblib"
    joblib.dump(bundle, model_file)

    # Nạp vào MLPredictor
    predictor = MLPredictor(model_path=model_file, fallback_to_baseline=True)
    assert predictor.model_loaded is True

    feat = {col: 1.0 for col in FEATURE_ORDER}
    hi_pred = predictor.predict_health(feat)
    assert isinstance(hi_pred, float)
    assert hi_pred >= 0.0

    rul_pred = predictor.predict_rul(feat, hi_pred, t=100.0)
    assert isinstance(rul_pred["rul_hours"], float)
    assert rul_pred["rul_hours"] >= 0.0
    assert predictor.fallback_count == 0


def test_create_predictor_factory() -> None:
    pred_base = create_predictor(mode="baseline")
    assert isinstance(pred_base, RuleBasedPredictor)

    pred_ai = create_predictor(mode="ai")
    assert isinstance(pred_ai, MLPredictor)
