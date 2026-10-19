"""Tính toán đặc trưng cửa sổ và chu kỳ nén."""

from __future__ import annotations

import math
from typing import Any


def window_features(samples: list[dict[str, Any]]) -> dict[str, float]:
    """Tính các đặc trưng theo cửa sổ từ chuỗi mẫu cảm biến.

    Tham số:
        samples: Danh sách dict mẫu, mỗi mẫu có các khóa:
            - TP2: áp suất máy nén (bar)
            - Motor_current: dòng điện động cơ (A)
            - Oil_temperature: nhiệt độ dầu (°C)
            - COMP: 0 nếu đang nạp khí (under load), 1 nếu không tải/tắt
            - ts: timestamp (giây hoặc float)

    Trả về:
        Dict chứa các đặc trưng:
            - tp2_mean, tp2_std, tp2_slope
            - loaded_ratio: tỷ lệ thời gian COMP == 0
            - rise_rate: tốc độ tăng áp trung bình khi tải (bar/giây)
            - cycle_len_s: độ dài chu kỳ nén ước tính (giây)
    """
    if not samples:
        return {
            "tp2_mean": 0.0,
            "tp2_std": 0.0,
            "tp2_slope": 0.0,
            "loaded_ratio": 0.0,
            "rise_rate": 0.0,
            "cycle_len_s": 0.0,
        }

    n = len(samples)
    tp2_vals = [float(s.get("TP2", 0.0)) for s in samples]
    comp_vals = [float(s.get("COMP", 1.0)) for s in samples]

    # 1. Trung bình & Độ lệch chuẩn TP2
    tp2_mean = sum(tp2_vals) / n
    if n > 1:
        variance = sum((x - tp2_mean) ** 2 for x in tp2_vals) / (n - 1)
        tp2_std = math.sqrt(max(0.0, variance))
    else:
        tp2_std = 0.0

    # 2. Độ dốc (Slope) theo phương pháp bình phương tối thiểu
    if n > 1:
        # Giả sử các mẫu cách đều nhau theo bước 1 đơn vị thời gian chuẩn hoá
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
    # Tìm các đoạn liên tiếp mà COMP == 0
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
    # Nếu kết thúc cửa sổ vẫn đang tải
    if seg_start != -1 and n > seg_start + 1:
        dp = tp2_vals[-1] - tp2_vals[seg_start]
        dt = float(n - 1 - seg_start)
        if dt > 0 and dp > 0:
            rise_rates.append(dp / dt)

    rise_rate = (sum(rise_rates) / len(rise_rates)) if rise_rates else max(0.0, tp2_slope)

    # 5. Độ dài chu kỳ nén (cycle_len_s)
    # Đếm số lần chuyển tiếp từ không tải (COMP >= 0.5) sang tải (COMP < 0.5)
    transitions = 0
    for i in range(1, n):
        if comp_vals[i - 1] >= 0.5 and comp_vals[i] < 0.5:
            transitions += 1
    cycle_len_s = (float(n) / transitions) if transitions > 0 else float(n)

    return {
        "tp2_mean": round(tp2_mean, 4),
        "tp2_std": round(tp2_std, 4),
        "tp2_slope": round(tp2_slope, 5),
        "loaded_ratio": round(loaded_ratio, 4),
        "rise_rate": round(rise_rate, 4),
        "cycle_len_s": round(cycle_len_s, 2),
    }
