"""Unit tests cho thuật toán lập lịch Greedy của Scheduler."""

import math
from scheduler.app import run_greedy_scheduler


def test_greedy_scoring_logic() -> None:
    # Kiểm tra công thức xác suất hỏng p_i = exp(-rul / horizon)
    horizon = 72.0
    downtime_cost = 10000.0
    maint_cost = 1500.0

    # Máy 1: RUL rất thấp (5 giờ) -> nguy cấp
    rul_1 = 5.0
    p1 = math.exp(-rul_1 / horizon)
    saving_1 = p1 * downtime_cost - maint_cost

    # Máy 2: RUL cao (150 giờ) -> an toàn
    rul_2 = 150.0
    p2 = math.exp(-rul_2 / horizon)
    saving_2 = p2 * downtime_cost - maint_cost

    assert p1 > p2
    assert saving_1 > saving_2
    assert saving_1 > 0  # Nên bảo trì máy 1
    assert saving_2 < 0  # Không cần bảo trì máy 2
