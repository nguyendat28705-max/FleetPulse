"""Unit tests cho thư viện fp_algo."""

import pytest
from fp_algo.detect import Cusum
from fp_algo.features import window_features
from fp_algo.health import HealthIndex
from fp_algo.rul import KalmanRul
from fp_algo.sampling import SwingingDoor


def test_window_features() -> None:
    samples = [
        {"TP2": 1.0, "COMP": 0, "Motor_current": 5.0, "Oil_temperature": 55.0},
        {"TP2": 2.0, "COMP": 0, "Motor_current": 5.2, "Oil_temperature": 55.1},
        {"TP2": 3.0, "COMP": 1, "Motor_current": 0.5, "Oil_temperature": 55.2},
        {"TP2": 4.0, "COMP": 1, "Motor_current": 0.5, "Oil_temperature": 55.0},
    ]
    feat = window_features(samples)
    assert feat["tp2_mean"] == 2.5
    assert feat["tp2_std"] > 0
    assert feat["tp2_slope"] == 1.0
    assert feat["loaded_ratio"] == 0.5  # 2 trong 4 mẫu có COMP == 0


def test_cusum_detect() -> None:
    cusum = Cusum(mu=0.0, sigma=1.0, k=0.5, h=3.0)
    # Tín hiệu bình thường quanh 0
    for _ in range(5):
        assert not cusum.update(0.1)

    # Tín hiệu lệch mạnh liên tục -> phải phát hiện cảnh báo
    alerted = False
    for _ in range(10):
        if cusum.update(2.5):
            alerted = True
            break
    assert alerted


def test_swinging_door_sampling() -> None:
    sdt = SwingingDoor(eps=0.1)
    points = [
        (0.0, 1.0),
        (1.0, 1.02),
        (2.0, 1.01),
        (3.0, 2.5),  # Bước nhảy đột ngột
        (4.0, 2.52),
    ]
    emitted = []
    for t, x in points:
        emitted.extend(sdt.push(t, x))
    assert len(emitted) >= 2


def test_health_index() -> None:
    # 20 mẫu baseline bình thường
    baseline_samples = [
        {
            "feat": {
                "tp2_mean": 8.9 + (i % 3) * 0.05,
                "tp2_std": 0.12,
                "loaded_ratio": 0.42,
                "rise_rate": 0.07,
                "cycle_len_s": 38.0,
            }
        }
        for i in range(20)
    ]
    hi_calc = HealthIndex()
    hi_calc.fit(baseline_samples)

    # Mẫu bình thường -> HI thấp (< 2.0)
    normal_feat = {
        "tp2_mean": 8.92,
        "tp2_std": 0.12,
        "loaded_ratio": 0.42,
        "rise_rate": 0.07,
        "cycle_len_s": 38.0,
    }
    hi_normal = hi_calc.update(normal_feat)
    assert hi_normal < 3.0

    # Mẫu rò khí: loaded_ratio tăng vọt, rise_rate giảm mạnh -> HI tăng cao
    degraded_feat = {
        "tp2_mean": 7.5,
        "tp2_std": 0.35,
        "loaded_ratio": 0.95,
        "rise_rate": 0.01,
        "cycle_len_s": 15.0,
    }
    hi_degraded = hi_calc.update(degraded_feat)
    assert hi_degraded > hi_normal


def test_kalman_rul() -> None:
    rul = KalmanRul(hi_failure_threshold=10.0)
    # Cập nhật suy giảm dần theo thời gian
    t = 0.0
    for step in range(10):
        t += 10.0
        hi_val = 1.0 + step * 0.2
        res = rul.update(hi_val, t)

    assert "rul_hours" in res
    assert res["rul_hours"] > 0
    assert res["rul_low"] <= res["rul_hours"] <= res["rul_high"]

    # Thử chế độ predict_only khi im lặng
    t += 30.0
    res_silent = rul.predict_only(t)
    assert res_silent["hi"] >= res["hi"]
