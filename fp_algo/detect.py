"""Thuật toán CUSUM (Cumulative Sum) phát hiện suy giảm sớm."""

from __future__ import annotations


class Cusum:
    """CUSUM phát hiện độ lệch trung bình tích lũy (hỗ trợ cả một phía và hai phía)."""

    def __init__(
        self,
        mu: float = 0.0,
        sigma: float = 1.0,
        k: float = 0.5,
        h: float = 5.0,
        two_sided: bool = True,
    ) -> None:
        """Khởi tạo CUSUM.

        Tham số:
            mu: Giá trị trung bình kỳ vọng ở trạng thái bình thường
            sigma: Độ lệch chuẩn bình thường
            k: Khoảng dung lượng / độ nhạy (mặc định 0.5 sigma)
            h: Ngưỡng cảnh báo (mặc định 5.0 sigma)
            two_sided: True nếu bắt cả lệch trên và dưới, False nếu chỉ bắt lệch dương (suy giảm)
        """
        self.mu = float(mu)
        self.sigma = float(sigma) if sigma > 1e-9 else 1.0
        self.k = float(k)
        self.h = float(h)
        self.two_sided = bool(two_sided)
        self.pos = 0.0
        self.neg = 0.0
        self.alert = False

    def update(self, x: float) -> bool:
        """Cập nhật với một mẫu mới và trả về True nếu vượt ngưỡng."""
        z = (x - self.mu) / self.sigma
        self.pos = max(0.0, self.pos + z - self.k)
        self.neg = min(0.0, self.neg + z + self.k)
        if self.two_sided:
            self.alert = (self.pos >= self.h) or (-self.neg >= self.h)
        else:
            self.alert = (self.pos >= self.h)
        return self.alert

    def reset(self) -> None:
        """Đặt lại bộ đếm tích lũy về 0."""
        self.pos = 0.0
        self.neg = 0.0
        self.alert = False
