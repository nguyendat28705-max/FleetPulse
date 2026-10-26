"""Kiểm thử tích hợp End-to-End: Raw Telemetry -> Gateway Window -> Ingestion -> DeviceProcessor -> Alert."""

import time
from datetime import datetime, timezone
import pytest

from engine.run import DeviceProcessor
from fp_algo.contract import FEATURE_ORDER
from fp_algo.features import window_features


def test_e2e_streaming_pipeline_flow() -> None:
    # 1. Giả lập luồng telemetry từ simulator
    device_id = "M-099"
    now_iso = datetime.now(timezone.utc).isoformat()
    raw_telemetry_batch = []
    for i in range(10):
        raw_telemetry_batch.append(
            {
                "deviceId": device_id,
                "line": "L1",
                "gatewayId": "GW-01",
                "TP2": 8.9 + (i * 0.02),
                "TP3": 8.95,
                "Motor_current": 3.2,
                "Oil_temperature": 62.0,
                "COMP": 0,
                "ts": now_iso,
            }
        )

    # 2. Gateway xử lý cửa sổ gom mẫu (Edge Gateway)
    feat = window_features(raw_telemetry_batch)
    for col in FEATURE_ORDER:
        assert col in feat

    gateway_feature_doc = {
        "deviceId": device_id,
        "line": "L1",
        "gatewayId": "GW-01",
        "windowStart": now_iso,
        "windowSec": 10,
        "n": len(raw_telemetry_batch),
        "feat": feat,
        "edge": {"alert": False, "mode": "normal"},
    }

    # 3. Ingestion gắn metadata & shard
    shard_id = 1
    ingest_doc = dict(gateway_feature_doc)
    ingest_doc["shard"] = shard_id
    ingest_doc["createdAt"] = datetime.now(timezone.utc)

    # 4. Streaming Engine xử lý qua DeviceProcessor (Chế độ Baseline)
    baseline_proc = DeviceProcessor(device_id=device_id, predictor_mode="baseline")
    state_baseline = baseline_proc.process_features(ingest_doc)

    assert state_baseline["deviceId"] == device_id
    assert state_baseline["status"] in ("ok", "degrading")
    assert state_baseline["hi"] >= 0.0
    assert state_baseline["rulHours"] > 0.0
    assert state_baseline["predictorMode"] == "baseline"

    # 5. Thử nghiệm với chế độ ML Predictor (tự động fallback nếu chưa có model tĩnh)
    ai_proc = DeviceProcessor(device_id=device_id, predictor_mode="ai")
    state_ai = ai_proc.process_features(ingest_doc)

    assert state_ai["deviceId"] == device_id
    assert state_ai["predictorMode"] == "ai"
    assert state_ai["hi"] >= 0.0
    assert state_ai["rulHours"] > 0.0
