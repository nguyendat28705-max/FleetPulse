"""Tính chỉ số suy giảm Health Index (HI) theo khoảng cách Mahalanobis."""

from __future__ import annotations

from typing import Any
import numpy as np


class HealthIndex:
    """Tính khoảng cách Mahalanobis so với baseline hoạt động bình thường."""

    FEATURE_KEYS = ["tp2_mean", "tp2_std", "loaded_ratio", "rise_rate", "cycle_len_s"]

    def __init__(self, baseline: dict[str, Any] | None = None) -> None:
        """Khởi tạo với baseline có sẵn hoặc học online từ cửa sổ khỏe mạnh."""
        self.feature_keys = self.FEATURE_KEYS
        self.mean: np.ndarray | None = None
        self.inv_cov: np.ndarray | None = None

        if baseline and "mean" in baseline and "inv_cov" in baseline:
            self.mean = np.array(baseline["mean"], dtype=float)
            self.inv_cov = np.array(baseline["inv_cov"], dtype=float)
        elif baseline and "samples" in baseline:
            self.fit(baseline["samples"])

    def fit(self, samples: list[dict[str, Any]]) -> None:
        """Học vector kỳ vọng và nghịch đảo hiệp phương sai từ tập mẫu khỏe mạnh."""
        if not samples:
            return

        matrix = []
        for s in samples:
            feat = s.get("feat", s)
            vec = [float(feat.get(k, 0.0)) for k in self.feature_keys]
            matrix.append(vec)

        data = np.array(matrix, dtype=float)
        self.mean = np.mean(data, axis=0)

        cov = np.cov(data, rowvar=False)
        # Thêm regularization nhỏ để chống suy biến ma trận (numerical stability)
        reg = 1e-4 * np.eye(len(self.feature_keys))
        if cov.ndim == 0:
            cov = np.array([[float(cov)]])
        cov += reg

        try:
            self.inv_cov = np.linalg.pinv(cov)
        except Exception:
            self.inv_cov = np.eye(len(self.feature_keys))

    def update(self, feat: dict[str, Any]) -> float:
        """Tính chỉ số HI cho một bản tin đặc trưng.

        HI = sqrt((x - mu)^T * Sigma^-1 * (x - mu))
        """
        if self.mean is None or self.inv_cov is None:
            # Nếu chưa có baseline, đặt mặc định an toàn ban đầu
            return 0.0

        target_feat = feat.get("feat", feat)
        vec = np.array([float(target_feat.get(k, 0.0)) for k in self.feature_keys], dtype=float)
        diff = vec - self.mean

        dist_sq = float(diff.T @ self.inv_cov @ diff)
        hi = float(np.sqrt(max(0.0, dist_sq)))
        return round(hi, 4)

    def export_baseline(self) -> dict[str, Any]:
        """Xuất baseline sang dict để lưu vào MongoDB hoặc cache."""
        return {
            "feature_keys": self.feature_keys,
            "mean": self.mean.tolist() if self.mean is not None else [],
            "inv_cov": self.inv_cov.tolist() if self.inv_cov is not None else [],
        }
