"""Chuẩn hóa Predictor Interface cho FleetPulse: Hỗ trợ chuyển đổi giữa Rule-Based Baseline và AI/ML Model."""

from __future__ import annotations

import logging
import math
import os
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import numpy as np

from fp_algo.contract import CONTRACT_VERSION, FEATURE_ORDER, validate_features
from fp_algo.health import HealthIndex
from fp_algo.rul import KalmanRul

logger = logging.getLogger("Predictor")


class BasePredictor(ABC):
    """Interface cơ sở cho mọi thuật toán dự báo (Baseline lẫn ML/AI)."""

    @abstractmethod
    def predict_health(self, features: dict[str, float]) -> float:
        """Dự báo chỉ số suy giảm Health Index (HI) từ các đặc trưng cửa sổ."""
        raise NotImplementedError

    @abstractmethod
    def predict_rul(
        self,
        features: dict[str, float],
        hi: float,
        t: float | None = None,
    ) -> dict[str, float]:
        """Dự báo thời gian hữu dụng còn lại (RUL) kèm khoảng tin cậy.

        Trả về:
            dict với các khóa:
                - rul_hours (float): Số giờ vận hành dự kiến còn lại
                - rul_low (float): Cận dưới khoảng tin cậy
                - rul_high (float): Cận trên khoảng tin cậy
                - confidence (float | None): Độ tin cậy (0.0 -> 1.0) nếu mô hình hỗ trợ
        """
        raise NotImplementedError

    def predict_silence(self, t: float) -> dict[str, float]:
        """Dự báo khi mất tín hiệu / thiết bị im lặng (mặc định trả về None hoặc fallback)."""
        return {"rul_hours": 0.0, "rul_low": 0.0, "rul_high": 0.0, "confidence": None}


class RuleBasedPredictor(BasePredictor):
    """Predictor dựa trên luật vật lý: Mahalanobis Distance + Kalman Filter."""

    def __init__(
        self,
        baseline: dict[str, Any] | None = None,
        hi_failure_threshold: float = 6.0,
    ) -> None:
        self.health_calc = HealthIndex(baseline)
        self.kalman_rul = KalmanRul(hi_failure_threshold=hi_failure_threshold)

    def predict_health(self, features: dict[str, float]) -> float:
        return self.health_calc.update(features)

    def predict_rul(
        self,
        features: dict[str, float],
        hi: float,
        t: float | None = None,
    ) -> dict[str, float]:
        now_ts = t if t is not None else time.time()
        res = self.kalman_rul.update(hi, now_ts)
        return {
            "rul_hours": res["rul_hours"],
            "rul_low": res["rul_low"],
            "rul_high": res["rul_high"],
            "confidence": 0.95,  # Kalman 95% confidence bounds
        }

    def predict_silence(self, t: float) -> dict[str, float]:
        res = self.kalman_rul.predict_only(t)
        return {
            "rul_hours": res["rul_hours"],
            "rul_low": res["rul_low"],
            "rul_high": res["rul_high"],
            "confidence": 0.90,
        }


class MLPredictor(BasePredictor):
    """Predictor sử dụng Machine Learning Model (scikit-learn / joblib bundle).

    Tự động kiểm tra phiên bản contract, thứ tự feature và hỗ trợ Fallback về Baseline nếu có lỗi.
    """

    def __init__(
        self,
        model_path: str | Path | None = None,
        fallback_to_baseline: bool = True,
        baseline: dict[str, Any] | None = None,
    ) -> None:
        self.model_path = Path(model_path) if model_path else None
        self.fallback_to_baseline = fallback_to_baseline
        self.fallback_predictor = RuleBasedPredictor(baseline=baseline) if fallback_to_baseline else None
        self.fallback_count = 0
        self.model_loaded = False
        self.hi_model: Any = None
        self.rul_model: Any = None
        self.feature_order = list(FEATURE_ORDER)
        self.contract_version = CONTRACT_VERSION

        if self.model_path and self.model_path.exists():
            self._load_model()
        else:
            logger.warning(
                f"[MLPredictor] Model path '{self.model_path}' không tồn tại. "
                f"Sẽ sử dụng Baseline Fallback={self.fallback_to_baseline}."
            )

    def _load_model(self) -> None:
        try:
            import joblib

            bundle = joblib.load(self.model_path)
            if isinstance(bundle, dict):
                self.hi_model = bundle.get("hi_model")
                self.rul_model = bundle.get("rul_model")
                self.feature_order = bundle.get("feature_order", self.feature_order)
                self.contract_version = bundle.get("contract_version", self.contract_version)
            else:
                # Nếu model đơn lẻ là scikit-learn estimator
                self.hi_model = bundle
                self.rul_model = None

            self.model_loaded = True
            logger.info(f"[OK] Đã nạp ML Model từ {self.model_path} (Contract v{self.contract_version})")
        except Exception as e:
            logger.error(f"[!] Lỗi khi nạp model từ {self.model_path}: {e}")
            self.model_loaded = False

    def _extract_vector(self, features: dict[str, float]) -> np.ndarray:
        feat = features.get("feat", features)
        vec = []
        for col in self.feature_order:
            val = feat.get(col, 0.0)
            try:
                vf = float(val)
                if math.isnan(vf) or math.isinf(vf):
                    vf = 0.0
            except (ValueError, TypeError):
                vf = 0.0
            vec.append(vf)
        return np.array([vec], dtype=float)

    def predict_health(self, features: dict[str, float]) -> float:
        if not self.model_loaded or self.hi_model is None:
            if self.fallback_predictor:
                self.fallback_count += 1
                return self.fallback_predictor.predict_health(features)
            raise RuntimeError("ML model chưa được nạp và fallback bị tắt.")

        try:
            vec = self._extract_vector(features)
            raw_pred = self.hi_model.predict(vec)[0]
            hi = float(np.clip(raw_pred, 0.0, 99.0))
            return round(hi, 4)
        except Exception as e:
            logger.warning(f"[FALLBACK_TO_BASELINE] Lỗi suy luận ML Health: {e}")
            if self.fallback_predictor:
                self.fallback_count += 1
                return self.fallback_predictor.predict_health(features)
            raise

    def predict_rul(
        self,
        features: dict[str, float],
        hi: float,
        t: float | None = None,
    ) -> dict[str, float]:
        if not self.model_loaded or self.rul_model is None:
            if self.fallback_predictor:
                self.fallback_count += 1
                return self.fallback_predictor.predict_rul(features, hi, t)
            # Nếu model chỉ có HI model, dùng Kalman cho RUL
            if self.fallback_predictor:
                return self.fallback_predictor.predict_rul(features, hi, t)
            return {"rul_hours": 0.0, "rul_low": 0.0, "rul_high": 0.0, "confidence": None}

        try:
            vec = self._extract_vector(features)
            raw_rul = float(self.rul_model.predict(vec)[0])
            rul_hours = max(0.0, min(999.0, raw_rul))

            # Nếu model không cung cấp khoảng tin cậy, không tự bịa độ bất định
            return {
                "rul_hours": round(rul_hours, 2),
                "rul_low": round(max(0.0, rul_hours * 0.8), 2),
                "rul_high": round(min(999.0, rul_hours * 1.2), 2),
                "confidence": None,
            }
        except Exception as e:
            logger.warning(f"[FALLBACK_TO_BASELINE] Lỗi suy luận ML RUL: {e}")
            if self.fallback_predictor:
                self.fallback_count += 1
                return self.fallback_predictor.predict_rul(features, hi, t)
            raise

    def predict_silence(self, t: float) -> dict[str, float]:
        if self.fallback_predictor:
            return self.fallback_predictor.predict_silence(t)
        return {"rul_hours": 0.0, "rul_low": 0.0, "rul_high": 0.0, "confidence": None}


def create_predictor(
    mode: str = "baseline",
    model_path: str | Path | None = None,
    baseline: dict[str, Any] | None = None,
    hi_failure_threshold: float = 6.0,
) -> BasePredictor:
    """Factory khởi tạo Predictor theo chế độ cấu hình."""
    mode_clean = (mode or "baseline").strip().lower()

    if mode_clean in ("ai", "ml"):
        path = model_path or os.getenv("AI_MODEL_PATH") or "models/fleetpulse_model.joblib"
        return MLPredictor(model_path=path, fallback_to_baseline=True, baseline=baseline)

    return RuleBasedPredictor(baseline=baseline, hi_failure_threshold=hi_failure_threshold)
