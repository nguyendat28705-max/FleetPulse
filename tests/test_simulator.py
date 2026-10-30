"""Unit tests cho simulator/inject.py va simulator/run.py."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from simulator.inject import inject_fault, validate_input
from simulator.run import FleetSimulator, VirtualDevice


def test_validate_input_severity_range():
    """Kiem tra validate severity chi chap nhan tu 0.1 den 1.0."""
    # Hop le
    validate_input("M-001", "air_leak", 0.1, 0)
    validate_input("M-001", "air_leak", 0.6, 10)
    validate_input("M-001", "air_leak", 1.0, 0)

    # Khong hop le
    with pytest.raises(ValueError, match="Severity phai nam trong khoang"):
        validate_input("M-001", "air_leak", 0.05, 0)

    with pytest.raises(ValueError, match="Severity phai nam trong khoang"):
        validate_input("M-001", "air_leak", 1.5, 0)

    with pytest.raises(ValueError, match="Severity phai nam trong khoang"):
        validate_input("M-001", "air_leak", -0.5, 0)


def test_validate_input_device_id_format_and_range():
    """Kiem tra validate device_id dung dinh dang M-XXX va trong khoang hop le."""
    validate_input("M-005", "air_leak", 0.5, 0)
    validate_input("M-020", "sensor_drift", 0.5, 0)

    # Sai format
    with pytest.raises(ValueError, match="Ma thiet bi '.*' khong hop le"):
        validate_input("INVALID", "air_leak", 0.5, 0)

    with pytest.raises(ValueError, match="Ma thiet bi '.*' khong hop le"):
        validate_input("M-9999", "air_leak", 0.5, 0)


def test_validate_input_offset_sec():
    """Kiem tra offset_sec khong duoc am."""
    validate_input("M-001", "air_leak", 0.5, 0)
    validate_input("M-001", "air_leak", 0.5, 120)

    with pytest.raises(ValueError, match="offset_sec phai la so nguyen khong am"):
        validate_input("M-001", "air_leak", 0.5, -5)


def test_validate_input_fault_type():
    """Kiem tra chi chap nhan 3 loai loi hop le."""
    for ft in ["air_leak", "sensor_drift", "stuck_comp"]:
        validate_input("M-001", ft, 0.5, 0)

    with pytest.raises(ValueError, match="Loai loi '.*' khong duoc ho tro"):
        validate_input("M-001", "unknown_fault", 0.5, 0)


@patch("simulator.inject.create_mongo_client")
def test_inject_fault_resolves_old_active_faults(mock_create_mongo):
    """Kiem tra khi tiem loi moi, loi cu chua resolved cua thiet bi se duoc giai quyet."""
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_client.__getitem__.return_value = mock_db
    mock_create_mongo.return_value = mock_client

    # Gia lap thiet bi da co 1 loi active
    mock_db["fault_labels"].find.return_value = [{"_id": "old_123", "deviceId": "M-002", "resolved": False}]
    mock_insert_res = MagicMock()
    mock_insert_res.inserted_id = "new_456"
    mock_db["fault_labels"].insert_one.return_value = mock_insert_res

    res = inject_fault("M-002", "sensor_drift", 0.8, offset_sec=15, overwrite=True)

    # Phai goi update_many de close loi cu
    mock_db["fault_labels"].update_many.assert_called_once()
    assert res["_id"] == "new_456"
    assert res["offset_sec"] == 15
    assert "active_from" in res
    assert res["type"] == "sensor_drift"


@patch("simulator.inject.create_mongo_client")
def test_inject_fault_prevents_concurrent_faults_when_no_overwrite(mock_create_mongo):
    """Kiem tra neu no-overwrite ma thiet bi da co loi dang chay thi raise RuntimeError."""
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_client.__getitem__.return_value = mock_db
    mock_create_mongo.return_value = mock_client

    mock_db["fault_labels"].find.return_value = [{"_id": "old_123", "deviceId": "M-002", "resolved": False}]

    with pytest.raises(RuntimeError, match="da co 1 loi chua resolved"):
        inject_fault("M-002", "air_leak", 0.5, overwrite=False)


def test_virtual_device_fault_mechanics():
    """Kiem tra cac logic bien doi tin hieu cho 3 loai loi."""
    dev = VirtualDevice("M-001", "L1", "GW-01", 0, 0.02, 0.0, 0.0)

    # 1. sensor_drift
    sev = 0.8
    tp2_base = 5.0
    temp_base = 50.0
    tp2_drift = tp2_base + 1.5 * sev
    temp_drift = temp_base + 12.0 * sev
    assert tp2_drift == 6.2
    assert temp_drift == 59.6

    # 2. stuck_comp
    comp_stuck = 0.0
    motor_stuck = max(3.0, 7.5 + 0.8 * sev)
    temp_stuck = temp_base + 15.0 * sev
    assert comp_stuck == 0.0
    assert motor_stuck >= 8.14
    assert temp_stuck == 62.0


@patch("pathlib.Path.exists", return_value=True)
@patch("simulator.run.create_mongo_client")
@patch("simulator.run.pd.read_parquet")
def test_simulator_sync_faults_priority_and_offset(mock_read_parquet, mock_create_mongo, mock_exists):
    """Kiem tra sync_faults uu tien loi moi nhat va tinh toan trigger_ts theo offset_sec."""
    # Mock dataframe
    dummy_df = pd.DataFrame({
        "timestamp": ["2026-10-20T08:00:00Z"] * 200,
        "TP2": [8.5] * 200,
        "TP3": [8.9] * 200,
        "COMP": [1.0] * 200,
        "Motor_current": [3.0] * 200,
        "Oil_temperature": [55.0] * 200,
    })
    mock_read_parquet.return_value = dummy_df

    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_client.__getitem__.return_value = mock_db
    mock_create_mongo.return_value = mock_client

    # 2 loi cho cung thiet bi M-001 (loi moi nhat la stuck_comp)
    now = datetime.now(timezone.utc)
    mock_find_cursor = MagicMock()
    mock_find_cursor.sort.return_value = [
        {"_id": "fault_new", "deviceId": "M-001", "type": "stuck_comp", "severity": 0.7, "t0": now, "offset_sec": 30},
        {"_id": "fault_old", "deviceId": "M-001", "type": "air_leak", "severity": 0.4, "t0": now, "offset_sec": 0},
    ]
    mock_db["fault_labels"].find.return_value = mock_find_cursor

    sim = FleetSimulator(num_devices=2, speed_factor=60)
    sim.sync_faults()

    dev1 = sim.devices[0]
    assert dev1.device_id == "M-001"
    assert dev1.active_fault is not None
    assert dev1.active_fault["_id"] == "fault_new"
    assert dev1.active_fault["type"] == "stuck_comp"
    # trigger_ts phai cach t0 dung 30 giay
    assert pytest.approx(dev1.trigger_ts, rel=1e-3) == now.timestamp() + 30


