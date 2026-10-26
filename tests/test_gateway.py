"""Unit tests cho Edge Gateway Runtime trong gateway/run.py."""

from __future__ import annotations

import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import paho.mqtt.client as mqtt
from gateway.run import EdgeGateway, is_valid_float


@pytest.fixture
def temp_gateway(tmp_path):
    """Khoi tao EdgeGateway voi database SQLite tam."""
    gw = EdgeGateway(
        gateway_id="GW-TEST",
        window_sec=10,
        max_queue_records=10,
        late_threshold_sec=5.0,
    )
    # Redirect db_path sang thu muc test tam
    gw.db_path = tmp_path / "gw_test_queue.db"
    gw._init_sqlite()
    return gw


def test_is_valid_float():
    assert is_valid_float(10.5) is True
    assert is_valid_float("8.9") is True
    assert is_valid_float(float("nan")) is False
    assert is_valid_float(float("inf")) is False
    assert is_valid_float("-inf") is False
    assert is_valid_float(None) is False
    assert is_valid_float("abc") is False


def test_validate_payload_rejects_invalid(temp_gateway):
    gw = temp_gateway

    # Thieu deviceId
    ok, err, _ = gw._validate_payload({"values": {"TP2": 8.0}})
    assert ok is False
    assert "deviceId" in err

    # deviceId rong
    ok, err, _ = gw._validate_payload({"deviceId": "  ", "values": {"TP2": 8.0}})
    assert ok is False

    # NaN trong cam bien
    ok, err, _ = gw._validate_payload({"deviceId": "M-001", "values": {"TP2": float("nan")}})
    assert ok is False
    assert "NaN/Inf" in err

    # Hop le
    ok, d_id, sample = gw._validate_payload({
        "deviceId": "M-001",
        "line": "L1",
        "values": {"TP2": 8.5, "TP3": 8.9, "COMP": 0, "Motor_current": 3.5, "Oil_temperature": 55.0},
        "ts": "2026-10-24T05:00:00Z",
    })
    assert ok is True
    assert d_id == "M-001"
    assert sample["TP2"] == 8.5


def test_flush_queue_preserves_records_on_publish_failure(temp_gateway):
    """P0-1: Kiem tra _flush_queue khong xoa ban ghi neu publish() tra ve rc khac MQTT_ERR_SUCCESS."""
    gw = temp_gateway
    gw.is_connected = True

    # Enqueue 1 ban ghi vao SQLite
    gw._enqueue(priority=0, topic="fleet/L1/M-001/features", payload_str='{"test": 1}')
    assert gw._get_queue_depth() == 1

    # Mock client.publish tra ve rc = MQTT_ERR_NO_CONN (4)
    mock_info = MagicMock()
    mock_info.rc = mqtt.MQTT_ERR_NO_CONN
    gw.client.publish = MagicMock(return_value=mock_info)

    gw._flush_queue()

    # Ban ghi van phai nam trong SQLite!
    assert gw._get_queue_depth() == 1
    assert gw.publish_failures == 1


def test_flush_queue_deletes_records_only_on_success(temp_gateway):
    """P0-1: Kiem tra _flush_queue chi xoa ban ghi khi publish() thanh cong."""
    gw = temp_gateway
    gw.is_connected = True

    gw._enqueue(priority=1, topic="fleet/L1/M-001/features", payload_str='{"test": 2}')
    assert gw._get_queue_depth() == 1

    mock_info = MagicMock()
    mock_info.rc = mqtt.MQTT_ERR_SUCCESS
    gw.client.publish = MagicMock(return_value=mock_info)

    gw._flush_queue()

    # Ban ghi da duoc xoa
    assert gw._get_queue_depth() == 0
    assert gw.publish_success == 1


def test_process_window_enqueues_when_publish_fails_while_connected(temp_gateway):
    """P0-2: Khi is_connected=True nhung publish() that bai, feature phai duoc enqueue vao SQLite."""
    gw = temp_gateway
    gw.is_connected = True

    # Nap sample vao buffer
    gw.buffers["M-001"] = [
        {"TP2": 8.0, "TP3": 8.9, "COMP": 0, "Motor_current": 3.0, "Oil_temperature": 50.0, "ts": "2026-10-24T05:00:00Z"}
    ]
    gw.window_starts["M-001"] = time.time() - 10.0

    # Mock publish fail
    mock_info = MagicMock()
    mock_info.rc = mqtt.MQTT_ERR_NO_CONN
    gw.client.publish = MagicMock(return_value=mock_info)

    gw._process_window("M-001")

    # Phai duoc luu vao SQLite
    assert gw._get_queue_depth() == 1
    assert gw.publish_failures == 1


def test_windowing_is_strictly_time_based(temp_gateway):
    """P1-3: Kiem tra cua so 10s khong bi dong som boi so luong message neu chua du thoi gian."""
    gw = temp_gateway
    gw.window_sec = 10
    gw._process_window = MagicMock()

    base_time = 1000.0
    with patch("time.time", return_value=base_time):
        msg = MagicMock()
        msg.payload = json.dumps({
            "deviceId": "M-001",
            "values": {"TP2": 8.0, "TP3": 8.9, "COMP": 1, "Motor_current": 3.0, "Oil_temperature": 55.0},
        }).encode("utf-8")

        # Gui 20 message trong vong 1 giay (toc do cao)
        for i in range(20):
            gw._on_message(None, None, msg)

    # Chua qua 10 giay thi _process_window KHONG duoc goi du da co 20 mau!
    gw._process_window.assert_not_called()
    assert len(gw.buffers["M-001"]) == 20

    # Sau 10.5 giay
    with patch("time.time", return_value=base_time + 10.5):
        gw._on_message(None, None, msg)
        gw._process_window.assert_called_once_with("M-001", is_idle_flush=False)


def test_buffer_not_cleared_on_feature_calculation_exception(temp_gateway):
    """P1-4: Kiem tra buffer khong bi xoa neu tinh dac trung gap exception."""
    gw = temp_gateway
    gw.buffers["M-001"] = [
        {"TP2": 8.0, "TP3": 8.9, "COMP": 0, "Motor_current": 3.0, "Oil_temperature": 50.0}
    ]

    with patch("gateway.run.window_features", side_effect=RuntimeError("Calculation Error")):
        gw._process_window("M-001")

    # Buffer van con nguyen ven
    assert len(gw.buffers["M-001"]) == 1
    assert gw.windows_failed == 1


def test_check_idle_windows_flushes_inactive_stream(temp_gateway):
    """P1-5: Kiem tra check_idle_windows tu dong dong cua so treo khi thiet bi dung gui tin."""
    gw = temp_gateway
    gw.is_connected = True
    gw.window_sec = 10
    now = time.time()

    gw.buffers["M-002"] = [
        {"TP2": 8.0, "TP3": 8.9, "COMP": 0, "Motor_current": 3.0, "Oil_temperature": 50.0, "ts": "2026-10-24T05:00:00Z"}
    ]
    # Bat dau tu 12 giay truoc, va message cuoi tu 3 giay truoc
    gw.window_starts["M-002"] = now - 12.0
    gw.last_seen["M-002"] = now - 3.0

    mock_info = MagicMock()
    mock_info.rc = mqtt.MQTT_ERR_SUCCESS
    gw.client.publish = MagicMock(return_value=mock_info)

    gw.check_idle_windows()

    # Buffer phai duoc flush xong
    assert len(gw.buffers["M-002"]) == 0
    # Kiem tra message publish co chua incomplete: True
    sent_payload = json.loads(gw.client.publish.call_args[0][1])
    assert sent_payload["incomplete"] is True


def test_late_arrival_detection(temp_gateway):
    """P2-7: Kiem tra phat hien late arrival dua tren emit_ts."""
    gw = temp_gateway
    gw.is_connected = True
    gw.late_threshold_sec = 5.0
    now = time.time()

    # Mau den tre 10 giay
    emit_dt = datetime.fromtimestamp(now - 10.0, tz=timezone.utc)
    gw.buffers["M-001"] = [
        {"TP2": 8.0, "TP3": 8.9, "COMP": 0, "Motor_current": 3.0, "Oil_temperature": 50.0, "emit_ts": emit_dt.isoformat()}
    ]
    gw.window_starts["M-001"] = now

    mock_info = MagicMock()
    mock_info.rc = mqtt.MQTT_ERR_SUCCESS
    gw.client.publish = MagicMock(return_value=mock_info)

    gw._process_window("M-001")

    sent_payload = json.loads(gw.client.publish.call_args[0][1])
    assert sent_payload["late"] is True
    assert sent_payload["ts_chain"]["latency_sec"] >= 9.5


def test_sqlite_capacity_limit_and_p0_protection(temp_gateway):
    """P2-8: Kiem tra hang doi SQLite xoa P2 khi day, tuyet doi khong xoa P0."""
    gw = temp_gateway
    gw.max_queue_records = 3

    # Nap 2 ban ghi P2 va 1 ban ghi P0
    gw._enqueue(priority=2, topic="t", payload_str="p2_old")
    gw._enqueue(priority=0, topic="t", payload_str="p0_alert")
    gw._enqueue(priority=2, topic="t", payload_str="p2_second")

    assert gw._get_queue_depth() == 3

    # Nap them ban ghi thu 4 (vuot nguong 3)
    gw._enqueue(priority=1, topic="t", payload_str="p1_new")

    # Ban ghi P0 tuyet doi khong bi xoa
    with sqlite3.connect(gw.db_path) as conn:
        p0_count = conn.execute("SELECT COUNT(*) FROM priority_queue WHERE priority = 0").fetchone()[0]
        assert p0_count == 1
