"""Thư viện thuật toán cốt lõi cho FleetPulse.

Bao gồm:
- features: Tính toán đặc trưng cửa sổ và chu kỳ nén
- detect: CUSUM phát hiện bất thường sớm
- sampling: Swinging Door Trending (SDT) lấy mẫu thích ứng
- health: Chỉ số suy giảm HI (khoảng cách Mahalanobis)
- rul: Bộ lọc Kalman ước lượng RUL và dự đoán khi mất tín hiệu
- predictor: Interface chuẩn hóa cho Baseline Rule-Based và AI/ML Serving
- contract: Data contract và kiểm thực đặc trưng
"""

from .contract import CONTRACT_VERSION, FEATURE_ORDER, validate_features
from .detect import Cusum
from .features import window_features
from .health import HealthIndex
from .predictor import (
    BasePredictor,
    MLPredictor,
    RuleBasedPredictor,
    create_predictor,
)
from .rul import KalmanRul
from .sampling import SwingingDoor

__all__ = [
    "Cusum",
    "window_features",
    "HealthIndex",
    "KalmanRul",
    "SwingingDoor",
    "BasePredictor",
    "RuleBasedPredictor",
    "MLPredictor",
    "create_predictor",
    "validate_features",
    "FEATURE_ORDER",
    "CONTRACT_VERSION",
]
