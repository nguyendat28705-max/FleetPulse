"""Kiểm thử Parity giữa Batch Feature Extraction và Online Gateway Feature Extraction.

Đảm bảo hai luồng sử dụng chung data contract, cùng logic tính toán và không có Training-Serving Skew.
"""

import math
import pytest
from fp_algo.contract import FEATURE_ORDER, validate_features
from fp_algo.features import window_features


def test_feature_parity_basic() -> None:
    """Kiểm tra cùng một chuỗi mẫu cảm biến cho kết quả giống hệt nhau."""
    raw_samples = [
        {"TP2": 1.0, "TP3": 9.0, "COMP": 0, "Motor_current": 5.0, "Oil_temperature": 55.0},
        {"TP2": 2.0, "TP3": 9.0, "COMP": 0, "Motor_current": 5.5, "Oil_temperature": 55.5},
        {"TP2": 3.0, "TP3": 9.0, "COMP": 1, "Motor_current": 0.5, "Oil_temperature": 56.0},
        {"TP2": 4.0, "TP3": 9.0, "COMP": 1, "Motor_current": 0.5, "Oil_temperature": 55.8},
        {"TP2": 5.0, "TP3": 9.0, "COMP": 0, "Motor_current": 6.0, "Oil_temperature": 56.5},
        {"TP2": 6.0, "TP3": 9.0, "COMP": 0, "Motor_current": 6.2, "Oil_temperature": 57.0},
    ]

    # Giả lập trích xuất ở Batch
    batch_feat = window_features(raw_samples)

    # Giả lập trích xuất tại Edge Gateway
    online_buffer = list(raw_samples)
    online_feat = window_features(online_buffer)

    # Kiểm tra từng feature
    for key in FEATURE_ORDER:
        assert key in batch_feat, f"Thiếu key {key} trong batch_feat"
        assert key in online_feat, f"Thiếu key {key} trong online_feat"
        assert math.isclose(batch_feat[key], online_feat[key], rel_tol=1e-5, abs_tol=1e-5), (
            f"Lệch giá trị tại feature {key}: batch={batch_feat[key]} vs online={online_feat[key]}"
        )

    # Xác thực contract
    is_valid, errors = validate_features(batch_feat)
    assert is_valid, f"Contract validation thất bại: {errors}"


def test_feature_parity_edge_cases() -> None:
    """Kiểm tra xử lý các trường hợp biên: thiếu trường, danh sách rỗng, 1 mẫu duy nhất."""
    # 1. Cửa sổ rỗng
    empty_feat = window_features([])
    assert len(empty_feat) == len(FEATURE_ORDER)
    for k in FEATURE_ORDER:
        assert empty_feat[k] == 0.0

    # 2. Một mẫu duy nhất (không đủ tính std hoặc slope)
    single_sample = [{"TP2": 8.9, "TP3": 8.9, "COMP": 0, "Motor_current": 5.0, "Oil_temperature": 60.0}]
    single_feat = window_features(single_sample)
    assert single_feat["tp2_mean"] == 8.9
    assert single_feat["tp2_std"] == 0.0
    assert single_feat["tp2_slope"] == 0.0
    assert single_feat["loaded_ratio"] == 1.0

    # 3. Thiếu một số trường cảm biến (tự động điền giá trị mặc định an toàn)
    missing_fields_sample = [
        {"TP2": 7.0},
        {"TP2": 7.5},
    ]
    missing_feat = window_features(missing_fields_sample)
    assert missing_feat["tp2_mean"] == 7.25
    assert missing_feat["oil_temp_mean"] == 60.0  # fallback mặc định
    assert missing_feat["motor_current_mean"] == 0.0
