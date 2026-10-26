"""Data Contract và kiểm thực đặc trưng (Feature Contract Validation) cho FleetPulse."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

CONTRACT_VERSION = "1.1.0"

FEATURE_METADATA: dict[str, dict[str, Any]] = {
    "tp2_mean": {
        "type": "float",
        "unit": "bar",
        "description": "Áp suất trung bình tại đầu ra máy nén trong cửa sổ",
        "source": "cảm biến analog TP2",
        "calculation": "mean(TP2)",
        "missing_rule": "fallback 0.0 hoặc baseline mean",
        "range": [-0.5, 15.0],
    },
    "tp2_std": {
        "type": "float",
        "unit": "bar",
        "description": "Độ biến thiên (độ lệch chuẩn mẫu N-1) của áp suất TP2",
        "source": "cảm biến analog TP2",
        "calculation": "std(TP2)",
        "missing_rule": "fallback 0.0",
        "range": [0.0, 10.0],
    },
    "tp2_slope": {
        "type": "float",
        "unit": "bar/sample",
        "description": "Độ dốc hồi quy tuyến tính OLS của áp suất TP2",
        "source": "cảm biến analog TP2",
        "calculation": "OLS slope",
        "missing_rule": "fallback 0.0",
        "range": [-5.0, 5.0],
    },
    "loaded_ratio": {
        "type": "float",
        "unit": "ratio",
        "description": "Tỷ lệ thời gian máy nén chạy có tải (COMP == 0)",
        "source": "tín hiệu số COMP",
        "calculation": "count(COMP < 0.5) / N",
        "missing_rule": "fallback 0.0",
        "range": [0.0, 1.0],
    },
    "rise_rate": {
        "type": "float",
        "unit": "bar/sample",
        "description": "Tốc độ tăng áp trung bình trong các pha chạy tải",
        "source": "TP2 và COMP",
        "calculation": "mean(delta_p / delta_t khi COMP == 0)",
        "missing_rule": "fallback max(0.0, tp2_slope)",
        "range": [0.0, 5.0],
    },
    "cycle_len_s": {
        "type": "float",
        "unit": "seconds",
        "description": "Độ dài chu kỳ nén ước tính giữa các lần bật tải",
        "source": "chuyển tiếp trạng thái COMP",
        "calculation": "N / count(transitions)",
        "missing_rule": "fallback N",
        "range": [0.0, 3600.0],
    },
    "oil_temp_mean": {
        "type": "float",
        "unit": "celsius",
        "description": "Nhiệt độ dầu bôi trơn trung bình của máy nén",
        "source": "cảm biến Oil_temperature",
        "calculation": "mean(Oil_temperature)",
        "missing_rule": "fallback 60.0",
        "range": [0.0, 120.0],
    },
    "oil_temp_max": {
        "type": "float",
        "unit": "celsius",
        "description": "Nhiệt độ dầu bôi trơn cao nhất trong cửa sổ",
        "source": "cảm biến Oil_temperature",
        "calculation": "max(Oil_temperature)",
        "missing_rule": "fallback 60.0",
        "range": [0.0, 130.0],
    },
    "motor_current_mean": {
        "type": "float",
        "unit": "amperes",
        "description": "Dòng điện động cơ trung bình của máy nén",
        "source": "cảm biến Motor_current",
        "calculation": "mean(Motor_current)",
        "missing_rule": "fallback 0.0",
        "range": [0.0, 25.0],
    },
    "motor_current_p95": {
        "type": "float",
        "unit": "amperes",
        "description": "Phân vị 95% dòng điện động cơ máy nén",
        "source": "cảm biến Motor_current",
        "calculation": "percentile_95(Motor_current)",
        "missing_rule": "fallback 0.0",
        "range": [0.0, 30.0],
    },
    "delta_pressure": {
        "type": "float",
        "unit": "bar",
        "description": "Chênh lệch áp suất TP2 (máy nén) và TP3 (bình điều khiển)",
        "source": "TP2 và TP3",
        "calculation": "mean(TP2) - mean(TP3)",
        "missing_rule": "fallback 0.0",
        "range": [-15.0, 15.0],
    },
}

FEATURE_ORDER = list(FEATURE_METADATA.keys())


def validate_features(feat_dict: dict[str, Any]) -> tuple[bool, list[str]]:
    """Kiểm tra tính hợp lệ của dict đặc trưng so với contract.

    Trả về:
        (is_valid: bool, errors: list[str])
    """
    errors: list[str] = []
    feat = feat_dict.get("feat", feat_dict)

    if not isinstance(feat, dict):
        return False, ["Dữ liệu feature không phải dict"]

    for name, meta in FEATURE_METADATA.items():
        if name not in feat:
            errors.append(f"Thiếu trường bắt buộc '{name}'")
            continue

        val = feat[name]
        try:
            vf = float(val)
            if math.isnan(vf) or math.isinf(vf):
                errors.append(f"Trường '{name}' có giá trị không hợp lệ (NaN hoặc Inf): {val}")
                continue

            min_v, max_v = meta["range"]
            if vf < min_v or vf > max_v:
                errors.append(f"Trường '{name}' vượt ngưỡng cho phép [{min_v}, {max_v}]: {vf}")
        except (ValueError, TypeError):
            errors.append(f"Trường '{name}' không thể chuyển thành float: {val}")

    return len(errors) == 0, errors


def export_contract_dict() -> dict[str, Any]:
    """Xuất toàn bộ schema contract ra dict."""
    return {
        "contract_version": CONTRACT_VERSION,
        "feature_order": FEATURE_ORDER,
        "features": FEATURE_METADATA,
    }
