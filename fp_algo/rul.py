"""Bộ lọc Kalman ước lượng RUL và dự đoán rủi ro khi im lặng."""

from __future__ import annotations

import math
from typing import Any
import numpy as np


class KalmanRul:
    """Bộ lọc Kalman 2 trạng thái: [HI, d(HI)/dt].

    Theo dõi mức suy giảm và tốc độ suy giảm để dự báo RUL
    (Remaining Useful Life) kèm khoảng tin cậy 95%.
    """

    def __init__(
        self,
        hi_failure_threshold: float = 8.0,
        q_hi: float = 1e-4,
        q_rate: float = 1e-5,
        r_hi: float = 0.05,
    ) -> None:
        """Khởi tạo Kalman RUL.

        Tham số:
            hi_failure_threshold: Ngưỡng HI coi là hỏng hoàn toàn (mặc định 8.0)
            q_hi: Nhiễu quá trình cho HI
            q_rate: Nhiễu quá trình cho tốc độ suy giảm
            r_hi: Nhiễu đo lường
        """
        self.threshold = float(hi_failure_threshold)
        # Vector trạng thái: [HI, v]
        self.x = np.array([0.0, 0.0], dtype=float)
        # Ma trận hiệp phương sai sai số
        self.P = np.array([[0.1, 0.0], [0.0, 0.01]], dtype=float)
        self.Q_base = np.array([[q_hi, 0.0], [0.0, q_rate]], dtype=float)
        self.R = np.array([[r_hi]], dtype=float)
        self.H = np.array([[1.0, 0.0]], dtype=float)
        self.last_t: float | None = None

    def update(self, hi: float, t: float) -> dict[str, float]:
        """Cập nhật với một quan sát HI mới tại thời điểm t (giây)."""
        if self.last_t is None:
            self.last_t = t
            self.x[0] = hi
            return self._compute_rul()

        dt = max(0.1, t - self.last_t)
        self.last_t = t

        # 1. Bước dự báo (Predict)
        F = np.array([[1.0, dt], [0.0, 1.0]], dtype=float)
        Q = self.Q_base * dt
        self.x = F @ self.x
        self.P = F @ self.P @ F.T + Q

        # 2. Bước hiệu chỉnh (Update)
        z = np.array([[hi]], dtype=float)
        y = z - (self.H @ self.x).reshape(1, 1)
        S = self.H @ self.P @ self.H.T + self.R
        K = self.P @ self.H.T @ np.linalg.inv(S)

        self.x = self.x + (K @ y).flatten()
        I = np.eye(2)
        self.P = (I - K @ self.H) @ self.P

        return self._compute_rul()

    def predict_only(self, t: float) -> dict[str, float]:
        """Chỉ chạy bước dự báo khi mất kết nối / thiết bị im lặng.

        Phương sai P sẽ tăng dần theo thời gian mất dữ liệu.
        """
        if self.last_t is None:
            self.last_t = t
            return self._compute_rul()

        dt = max(0.1, t - self.last_t)
        self.last_t = t

        F = np.array([[1.0, dt], [0.0, 1.0]], dtype=float)
        Q = self.Q_base * dt
        self.x = F @ self.x
        self.P = F @ self.P @ F.T + Q

        return self._compute_rul()

    def _compute_rul(self) -> dict[str, float]:
        """Tính RUL (giờ) và khoảng tin cậy [rul_low, rul_high]."""
        hi = max(0.0, float(self.x[0]))
        rate = max(1e-6, float(self.x[1]))  # tốc độ suy giảm per second
        hi_var = max(1e-6, float(self.P[0, 0]))
        rate_var = max(1e-8, float(self.P[1, 1]))

        remaining_hi = max(0.0, self.threshold - hi)
        if remaining_hi <= 0.0:
            return {"rul_hours": 0.0, "rul_low": 0.0, "rul_high": 0.0, "hi": round(hi, 3)}

        # RUL tính bằng giây -> chuyển sang giờ
        rul_sec = remaining_hi / rate
        rul_hours = rul_sec / 3600.0

        # Ước tính độ bất định của RUL theo vi phân gần đúng
        sigma_rul = (math.sqrt(hi_var) / rate + remaining_hi * math.sqrt(rate_var) / (rate**2)) / 3600.0
        rul_low = max(0.0, rul_hours - 1.96 * sigma_rul)
        rul_high = min(999.0, rul_hours + 1.96 * sigma_rul)

        return {
            "rul_hours": round(min(999.0, rul_hours), 2),
            "rul_low": round(rul_low, 2),
            "rul_high": round(rul_high, 2),
            "hi": round(hi, 3),
        }
