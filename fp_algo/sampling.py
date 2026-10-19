"""Thuật toán nén chuỗi thời gian Swinging Door Trending (SDT)."""

from __future__ import annotations


class SwingingDoor:
    """Swinging Door Trending (SDT) để lấy mẫu thích ứng."""

    def __init__(self, eps: float = 0.05) -> None:
        """Khởi tạo SDT.

        Tham số:
            eps: Sai số biên độ cho phép (compression deviation)
        """
        self.eps = float(eps)
        self.t0: float | None = None
        self.x0: float | None = None
        self.prev_t: float | None = None
        self.prev_x: float | None = None
        self.upper_slope: float = float("inf")
        self.lower_slope: float = float("-inf")

    def push(self, t: float, x: float) -> list[tuple[float, float]]:
        """Đẩy một điểm (t, x) vào thuật toán.

        Trả về:
            Danh sách các điểm cần phát ra (nếu có vi phạm biên).
        """
        emitted: list[tuple[float, float]] = []

        if self.t0 is None:
            self.t0 = t
            self.x0 = x
            self.prev_t = t
            self.prev_x = x
            self.upper_slope = float("inf")
            self.lower_slope = float("-inf")
            emitted.append((t, x))
            return emitted

        dt = t - self.t0
        if dt <= 0:
            return emitted

        # Tính góc mở hiện tại
        curr_upper = (x - (self.x0 + self.eps)) / dt
        curr_lower = (x - (self.x0 - self.eps)) / dt

        # Cửa chỉ khép lại (thu hẹp)
        new_upper = min(self.upper_slope, curr_upper)
        new_lower = max(self.lower_slope, curr_lower)

        # Nếu hai cánh cửa cắt nhau -> cần phát điểm trước đó
        if new_upper < new_lower:
            if self.prev_t is not None and self.prev_x is not None:
                emitted.append((self.prev_t, self.prev_x))
            # Điểm trước đó trở thành mốc mới
            self.t0 = self.prev_t if self.prev_t is not None else t
            self.x0 = self.prev_x if self.prev_x is not None else x
            dt_new = t - self.t0
            if dt_new > 0 and self.x0 is not None:
                self.upper_slope = (x - (self.x0 + self.eps)) / dt_new
                self.lower_slope = (x - (self.x0 - self.eps)) / dt_new
            else:
                self.upper_slope = float("inf")
                self.lower_slope = float("-inf")
        else:
            self.upper_slope = new_upper
            self.lower_slope = new_lower

        self.prev_t = t
        self.prev_x = x
        return emitted
