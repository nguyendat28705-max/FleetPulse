"""Tính toán đặc trưng cửa sổ và chu kỳ nén dùng chung cho cả Batch và Streaming."""

from __future__ import annotations

import math
from typing import Any

FEATURE_ORDER = [
    "tp2_mean",
    "tp2_std",
    "tp2_slope",
    "loaded_ratio",
    "rise_rate",
    "cycle_len_s",
    "oil_temp_mean",
    "oil_temp_max",
    "motor_current_mean",
    "motor_current_p95",
    "delta_pressure",
]


def window_features(samples: list[dict[str, Any]]) -> dict[str, float]:
    """Tính các đặc trưng theo cửa sổ từ chuỗi mẫu cảm biến.

    Tham số:
        samples: Danh sách dict mẫu, mỗi mẫu có các khóa:
            - TP2: áp suất máy nén (bar)
            - TP3: áp suất bình điều khiển / sau sấy khí (bar)
            - Motor_current: dòng điện động cơ (A)
            - Oil_temperature: nhiệt độ dầu (°C)
            - COMP: 0 nếu đang nạp khí (under load), 1 nếu không tải/tắt
            - ts: timestamp (giây, ISO string hoặc datetime)

    Trả về:
        Dict chứa 11 đặc trưng chuẩn hoá:
            - tp2_mean, tp2_std, tp2_slope
            - loaded_ratio, rise_rate, cycle_len_s
            - oil_temp_mean, oil_temp_max
            - motor_current_mean, motor_current_p95
            - delta_pressure (TP2 - TP3)
    """
    if not samples:
        return {k: 0.0 for k in FEATURE_ORDER}

    n = len(samples)

    # Trích xuất an toàn các cảm biến
    tp2_vals = [float(s.get("TP2", 0.0) or 0.0) for s in samples]
    tp3_vals = [float(s.get("TP3", 8.9) if s.get("TP3") is not None else 8.9) for s in samples]
    comp_vals = [float(s.get("COMP", 1.0) if s.get("COMP") is not None else 1.0) for s in samples]
    motor_vals = [float(s.get("Motor_current", 0.0) or 0.0) for s in samples]
    oil_vals = [float(s.get("Oil_temperature", 60.0) if s.get("Oil_temperature") is not None else 60.0) for s in samples]

    # 1. Trung bình & Độ lệch chuẩn TP2
    tp2_mean = sum(tp2_vals) / n
    if n > 1:
        variance = sum((x - tp2_mean) ** 2 for x in tp2_vals) / (n - 1)
        tp2_std = math.sqrt(max(0.0, variance))
    else:
        tp2_std = 0.0

    # 2. Độ dốc (Slope) theo phương pháp OLS
    if n > 1:
        x_mean = (n - 1) / 2.0
        denom = sum((i - x_mean) ** 2 for i in range(n))
        if denom > 1e-9:
            numer = sum((i - x_mean) * (tp2_vals[i] - tp2_mean) for i in range(n))
            tp2_slope = numer / denom
        else:
            tp2_slope = 0.0
    else:
        tp2_slope = 0.0

    # 3. Tỷ lệ thời gian tải (COMP == 0 là đang chạy tải nén khí)
    loaded_count = sum(1 for c in comp_vals if c < 0.5)
    loaded_ratio = loaded_count / float(n)

    # 4. Tốc độ tăng áp (rise_rate) trong các pha tải
    rise_rates: list[float] = []
    seg_start = -1
    for i, c in enumerate(comp_vals):
        if c < 0.5:
            if seg_start == -1:
                seg_start = i
        else:
            if seg_start != -1 and i > seg_start + 1:
                dp = tp2_vals[i - 1] - tp2_vals[seg_start]
                dt = float(i - seg_start)
                if dt > 0 and dp > 0:
                    rise_rates.append(dp / dt)
            seg_start = -1
    if seg_start != -1 and n > seg_start + 1:
        dp = tp2_vals[-1] - tp2_vals[seg_start]
        dt = float(n - 1 - seg_start)
        if dt > 0 and dp > 0:
            rise_rates.append(dp / dt)

    rise_rate = (sum(rise_rates) / len(rise_rates)) if rise_rates else max(0.0, tp2_slope)

    # 5. Độ dài chu kỳ nén (cycle_len_s)
    transitions = 0
    for i in range(1, n):
        if comp_vals[i - 1] >= 0.5 and comp_vals[i] < 0.5:
            transitions += 1
    cycle_len_s = (float(n) / transitions) if transitions > 0 else float(n)

    # 6. Nhiệt độ dầu
    oil_temp_mean = sum(oil_vals) / n
    oil_temp_max = max(oil_vals)

    # 7. Dòng điện động cơ
    motor_current_mean = sum(motor_vals) / n
    sorted_motor = sorted(motor_vals)
    p95_idx = int(math.ceil(0.95 * n)) - 1
    motor_current_p95 = sorted_motor[max(0, min(p95_idx, n - 1))]

    # 8. Chênh lệch áp suất TP2 - TP3
    delta_pressure = tp2_mean - (sum(tp3_vals) / n)

    return {
        "tp2_mean": round(tp2_mean, 4),
        "tp2_std": round(tp2_std, 4),
        "tp2_slope": round(tp2_slope, 5),
        "loaded_ratio": round(loaded_ratio, 4),
        "rise_rate": round(rise_rate, 4),
        "cycle_len_s": round(cycle_len_s, 2),
        "oil_temp_mean": round(oil_temp_mean, 2),
        "oil_temp_max": round(oil_temp_max, 2),
        "motor_current_mean": round(motor_current_mean, 3),
        "motor_current_p95": round(motor_current_p95, 3),
        "delta_pressure": round(delta_pressure, 4),
    }
