"""Unit tests cho logic phân tích trạng thái, incident lifecycle và concurrency của Streaming Engine."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from engine.run import DeviceProcessor, StreamingEngine


def test_device_processor_healthy_to_degraded() -> None:
    proc = DeviceProcessor("M-001", predictor_mode="baseline")

    # 1. Cửa sổ bình thường
    healthy_doc = {
        "windowStart": "2026-10-26T03:00:00Z",
        "feat": {
            "tp2_mean": 1.415,
            "tp2_std": 3.28,
            "loaded_ratio": 0.42,
            "rise_rate": 0.07,
            "cycle_len_s": 38.0,
        },
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
    # Cập nhật liên tiếp vài lần với windowStart khác nhau để CUSUM tăng
    for i in range(5):
        doc = dict(degraded_doc)
        doc["windowStart"] = f"2026-10-26T03:0{i}:00Z"
        state = proc.process_features(doc)

    assert state["status"] == "degrading"
    assert state["hi"] > 3.0


def test_device_processor_silent_failure_preserves_last_seen() -> None:
    """P0-2: Kiem tra lastSeen giu nguyen thoi diem nhan tin cuoi, khong bi cap nhat thanh now()."""
    proc = DeviceProcessor("M-002")

    # Gửi 1 bản tin để thiết lập mốc
    event_time_str = "2026-10-26T02:00:00Z"
    proc.process_features({
        "windowStart": event_time_str,
        "feat": {
            "tp2_mean": 1.415,
            "tp2_std": 3.28,
            "loaded_ratio": 0.42,
            "rise_rate": 0.07,
            "cycle_len_s": 38.0,
        },
    })

    # Giả lập thời gian trôi qua 35 giây (vượt ngưỡng 25s)
    proc.last_seen_wall = time.time() - 35.0

    state = proc.process_silence()
    assert state is not None
    assert state["status"] == "silent"
    assert state["deviceId"] == "M-002"

    # QUAN TRONG: lastSeen phai la 02:00:00 (thoi diem su kien cuoi cung), khong phai gio hien tai
    assert isinstance(state["lastSeen"], datetime)
    assert state["lastSeen"].hour == 2
    assert state["lastSeen"].minute == 0


def test_device_processor_idempotent_cusum_on_duplicate_window() -> None:
    """P1-7: Kiem tra cung 1 windowStart update lai khong bi tinh CUSUM 2 lan."""
    proc = DeviceProcessor("M-003")

    doc = {
        "windowStart": "2026-10-26T03:10:00Z",
        "feat": {
            "tp2_mean": 0.5,
            "tp2_std": 0.1,
            "loaded_ratio": 0.95,
            "rise_rate": 0.01,
            "cycle_len_s": 15.0,
        },
    }

    # Lan 1
    state1 = proc.process_features(doc)
    cusum_pos_1 = proc.cusum.pos

    # Lan 2 (cung windowStart, chi co ingest_ts thay doi)
    state2 = proc.process_features(doc)
    cusum_pos_2 = proc.cusum.pos

    # CUSUM khong duoc tich luy them
    assert cusum_pos_1 == cusum_pos_2


@patch("engine.run.create_mongo_client")
def test_sync_state_incident_lifecycle(mock_create_mongo) -> None:
    """P0-1: Kiem tra mo incident khi degrading va tu dong DONG incident khi ve ok."""
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_state_col = MagicMock()
    mock_incidents_col = MagicMock()
    mock_checkpoints_col = MagicMock()

    mock_db.__getitem__.side_effect = lambda name: {
        "device_state": mock_state_col,
        "incidents": mock_incidents_col,
        "engine_checkpoints": mock_checkpoints_col,
    }.get(name, MagicMock())

    mock_client.__getitem__.return_value = mock_db
    mock_create_mongo.return_value = mock_client

    engine = StreamingEngine(shard_id="all", mongo_uri="mongodb://localhost:27017", db_name="test_db")

    # 1. Thiet bi chuyen sang degrading -> phai mo/upsert active incident
    degrading_state = {
        "deviceId": "M-017",
        "status": "degrading",
        "hi": 4.2,
        "rulHours": 24.0,
        "predictorMode": "baseline",
    }
    engine._sync_state(degrading_state)

    # Kiem tra update_one tren incidents_col
    mock_incidents_col.update_one.assert_called_once()
    call_filter = mock_incidents_col.update_one.call_args[0][0]
    assert call_filter["deviceId"] == "M-017"
    assert call_filter["status"] == "active"

    # 2. Thiet bi phuc hoi ve ok -> phai DONG incident active (chuyen thanh resolved)
    mock_incidents_col.reset_mock()
    mock_update_res = MagicMock()
    mock_update_res.modified_count = 1
    mock_incidents_col.update_many.return_value = mock_update_res

    ok_state = {
        "deviceId": "M-017",
        "status": "ok",
        "hi": 1.2,
        "rulHours": 120.0,
        "predictorMode": "baseline",
    }
    engine._sync_state(ok_state)

    mock_incidents_col.update_many.assert_called_once()
    resolve_filter = mock_incidents_col.update_many.call_args[0][0]
    resolve_update = mock_incidents_col.update_many.call_args[0][1]

    assert resolve_filter["deviceId"] == "M-017"
    assert resolve_filter["status"] == "active"
    assert resolve_update["$set"]["status"] == "resolved"
    assert "resolvedAt" in resolve_update["$set"]


@patch("engine.run.create_mongo_client")
def test_pipeline_shard_mutual_exclusion(mock_create_mongo) -> None:
    """P1-6: Kiem tra bo loc shard khong bi tranh chap document."""
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_client.__getitem__.return_value = mock_db
    mock_create_mongo.return_value = mock_client

    # Shard 0 phu trach ca document khong co truong shard
    engine_0 = StreamingEngine(shard_id=0, total_shards=4)
    p0 = engine_0._build_pipeline()[0]["$match"]
    assert "$or" in p0
    assert {"fullDocument.shard": 0} in p0["$or"]
    assert {"fullDocument.shard": {"$exists": False}} in p0["$or"]

    # Shard 1 chi phu trach dung shard 1
    engine_1 = StreamingEngine(shard_id=1, total_shards=4)
    p1 = engine_1._build_pipeline()[0]["$match"]
    assert p1["fullDocument.shard"] == 1
    assert "$or" not in p1

    # Shard all phu trach tat ca
    engine_all = StreamingEngine(shard_id="all")
    p_all = engine_all._build_pipeline()[0]["$match"]
    assert "fullDocument.shard" not in p_all


@patch("engine.run.create_mongo_client")
def test_resume_token_checkpoint_save_and_load(mock_create_mongo) -> None:
    """P1-4: Kiem tra luu va doc resume_token tu engine_checkpoints."""
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_client.__getitem__.return_value = mock_db
    mock_create_mongo.return_value = mock_client

    engine = StreamingEngine(shard_id=2)

    # 1. Luu token
    dummy_token = {"_data": "dummy_resume_token_123"}
    engine._save_checkpoint(dummy_token)

    mock_db["engine_checkpoints"].update_one.assert_called_once()
    saved_filter = mock_db["engine_checkpoints"].update_one.call_args[0][0]
    saved_set = mock_db["engine_checkpoints"].update_one.call_args[0][1]["$set"]
    assert saved_filter["_id"] == "shard_2"
    assert saved_set["resume_token"] == dummy_token

    # 2. Doc token
    mock_db["engine_checkpoints"].find_one.return_value = {"_id": "shard_2", "resume_token": dummy_token}
    loaded = engine._load_checkpoint()
    assert loaded == dummy_token

