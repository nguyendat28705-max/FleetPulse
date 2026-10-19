"""Unit tests cho logic phân tích trạng thái và silent failure của Streaming Engine."""

import time
from engine.run import DeviceProcessor


def test_device_processor_healthy_to_degraded() -> None:
    proc = DeviceProcessor("M-001")

    # 1. Cửa sổ bình thường
    healthy_doc = {
        "feat": {
            "tp2_mean": 1.415,
            "tp2_std": 3.28,
            "loaded_ratio": 0.42,
            "rise_rate": 0.07,
            "cycle_len_s": 38.0,
        }
    }
    state = proc.process_features(healthy_doc)
    assert state["status"] == "ok"
    assert state["hi"] < 3.0
    assert state["rulHours"] > 0

    # 2. Cửa sổ rò khí nặng (loaded_ratio cao vọt, rise_rate tụt)
    degraded_doc = {
        "feat": {
            "tp2_mean": 0.5,
            "tp2_std": 0.1,
            "loaded_ratio": 0.98,
            "rise_rate": 0.005,
            "cycle_len_s": 10.0,
        }
    }
    # Cập nhật liên tiếp vài lần để CUSUM hoặc HI tăng
    for _ in range(5):
        state = proc.process_features(degraded_doc)

    assert state["status"] == "degrading"
    assert state["hi"] > 3.0


def test_device_processor_silent_failure() -> None:
    proc = DeviceProcessor("M-002")

    # Gửi 1 bản tin để thiết lập mốc
    proc.process_features({
        "feat": {
            "tp2_mean": 1.415,
            "tp2_std": 3.28,
            "loaded_ratio": 0.42,
            "rise_rate": 0.07,
            "cycle_len_s": 38.0,
        }
    })

    # Giả lập thời gian trôi qua 35 giây (vượt ngưỡng 25s)
    proc.last_seen_wall = time.time() - 35.0

    state = proc.process_silence()
    assert state is not None
    assert state["status"] == "silent"
    assert state["deviceId"] == "M-002"
