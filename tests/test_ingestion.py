"""Unit tests cho Ingestion Service trong ingestion/run.py."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pymongo.errors import BulkWriteError, PyMongoError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingestion.run import IngestionService, format_canonical_iso, parse_utc_iso


def test_parse_utc_iso():
    # Hop le UTC
    dt = parse_utc_iso("2026-10-24T05:00:00Z")
    assert dt is not None
    assert dt.tzinfo == timezone.utc
    assert dt.hour == 5

    # Hop le offset +07:00 -> ve UTC la 03:00
    dt7 = parse_utc_iso("2026-10-24T10:00:00+07:00")
    assert dt7 is not None
    assert dt7.astimezone(timezone.utc).hour == 3

    # Naive chuoi -> tu dong gan UTC
    dt_naive = parse_utc_iso("2026-10-24T08:00:00")
    assert dt_naive is not None
    assert dt_naive.tzinfo == timezone.utc

    # Khong hop le
    assert parse_utc_iso("invalid_time") is None
    assert parse_utc_iso("") is None
    assert parse_utc_iso(None) is None
    assert parse_utc_iso(12345) is None


def test_format_canonical_iso():
    dt = datetime(2026, 10, 24, 5, 0, 0, 123456, tzinfo=timezone.utc)
    res = format_canonical_iso(dt)
    assert res == "2026-10-24T05:00:00.123Z"


@pytest.fixture
def mock_ingestion():
    with patch("ingestion.run.create_mongo_client") as mock_create:
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        mock_create.return_value = mock_client

        service = IngestionService(
            mongo_uri="mongodb://localhost:27017",
            db_name="test_db",
            batch_size=5,
            flush_interval=1.0,
        )
        return service


def test_handle_telemetry_schema_protection_and_validation(mock_ingestion):
    """P1-4: Kiem tra khong de values ghi de cac truong metadata va loc NaN."""
    service = mock_ingestion

    # Thu gui values chua key co tinh ghi de 'ts' va 'meta'
    malicious_payload = {
        "deviceId": "M-001",
        "line": "L1",
        "seq": 100,
        "emit_ts": "2026-10-24T05:00:00Z",
        "values": {
            "ts": "hacked_ts",
            "meta": "hacked_meta",
            "TP2": 8.5,
            "Motor_current": float("nan"),  # NaN phai bi loai bo
            "unknown_sensor": 999.9,        # Ngoai whitelist bi loai bo
        },
    }

    service._handle_telemetry(malicious_payload)

    assert len(service.telemetry_batch) == 1
    doc = service.telemetry_batch[0]

    # ts va meta khong bi ghi de
    assert isinstance(doc["ts"], datetime)
    assert doc["meta"]["deviceId"] == "M-001"
    assert doc["meta"]["line"] == "L1"
    assert doc["seq"] == 100

    # TP2 duoc giu lai, Motor_current (NaN) va unknown_sensor bi loai
    assert doc["TP2"] == 8.5
    assert "Motor_current" not in doc
    assert "unknown_sensor" not in doc


def test_handle_telemetry_deduplication(mock_ingestion):
    """P1-3: Kiem tra co che chong ghi trung (Idempotency)."""
    service = mock_ingestion

    payload = {
        "deviceId": "M-001",
        "seq": 1,
        "ts": "2026-10-24T05:00:00Z",
        "values": {"TP2": 8.5},
    }

    # Gui lan 1
    service._handle_telemetry(payload)
    assert len(service.telemetry_batch) == 1
    assert service.duplicates_dropped == 0

    # Gui lan 2 (trung lap hoan toan)
    service._handle_telemetry(payload)
    assert len(service.telemetry_batch) == 1  # Khong tang them
    assert service.duplicates_dropped == 1


def test_flush_telemetry_preserves_batch_on_network_error(mock_ingestion):
    """P0-1: Kiem tra giu nguyen batch khi PyMongoError de retry sau."""
    service = mock_ingestion
    service.telemetry_batch = [{"doc": 1}, {"doc": 2}]

    # Mock insert_many nem loi PyMongoError
    service.telemetry_col.insert_many.side_effect = PyMongoError("Connection timed out")

    service.flush_telemetry()

    # Batch van phai duoc bao toan de retry
    assert len(service.telemetry_batch) == 2
    assert service.telemetry_failed == 2


def test_flush_telemetry_handles_partial_bulk_write_error(mock_ingestion):
    """P0-1: Kiem tra xu ly BulkWriteError chi retry nhung ban ghi loi khong phai duplicate."""
    service = mock_ingestion
    service.telemetry_batch = [{"_id": 1}, {"_id": 2}, {"_id": 3}]

    # Gia lap ban ghi 0 thanh cong, ban ghi 1 loi 11000 (duplicate), ban ghi 2 loi nghiem trong (network/validation)
    bwe_details = {
        "nInserted": 1,
        "writeErrors": [
            {"index": 1, "code": 11000, "errmsg": "E11000 duplicate key error"},
            {"index": 2, "code": 13000, "errmsg": "Write concern error"},
        ],
    }
    service.telemetry_col.insert_many.side_effect = BulkWriteError(bwe_details)

    service.flush_telemetry()

    # Ban ghi so 2 phai duoc giu lai de retry, ban ghi 0 va 1 khong retry
    assert len(service.telemetry_batch) == 1
    assert service.telemetry_batch[0]["_id"] == 3  # index 2 la ban ghi {_id: 3}
    assert service.duplicates_dropped == 1


def test_handle_features_safe_nested_dict_and_canonical_iso(mock_ingestion):
    """P1-5 & P1-6: Kiem tra an toan khi ts_chain/feat la None va chuan hoa ISO."""
    service = mock_ingestion

    payload = {
        "deviceId": "M-005",
        "windowStart": "2026-10-24T05:00:00+00:00",
        "ts_chain": None,  # Can khong bi crash
        "feat": None,
        "edge": None,
    }

    service._handle_features(payload)

    # update_one phai duoc goi
    assert service.features_col.update_one.called
    call_args = service.features_col.update_one.call_args
    filter_doc = call_args[0][0]
    set_doc = call_args[0][1]["$set"]

    # windowStart duoc chuan hoa ve Canonical ISO .000Z
    assert filter_doc["windowStart"] == "2026-10-24T05:00:00.000Z"
    # ts_chain khong bi crash va da co ingest_ts
    assert isinstance(set_doc["ts_chain"], dict)
    assert "ingest_ts" in set_doc["ts_chain"]
    assert service.features_upserted == 1
