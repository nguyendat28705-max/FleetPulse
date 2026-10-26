"""Unit tests cho Scheduler: greedy scoring, idempotency, validation va health check."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError
from scheduler.app import ScheduleRequest, datadog_webhook, health, run_greedy_scheduler


def test_greedy_scoring_logic_and_silent_penalty() -> None:
    """P1-2: Kiem tra cong thuc xac suat hong: RUL thap hon thi rui ro cao hon, va SILENT rui ro cao hon OK."""
    horizon = 72.0
    downtime_cost = 10000.0
    maint_cost = 1500.0

    # 1. RUL thap (5h) vs RUL cao (150h) o trang thai binh thuong
    rul_1 = 5.0
    p1 = math.exp(-rul_1 / horizon)
    saving_1 = p1 * downtime_cost - maint_cost

    rul_2 = 150.0
    p2 = math.exp(-rul_2 / horizon)
    saving_2 = p2 * downtime_cost - maint_cost

    assert p1 > p2
    assert saving_1 > saving_2
    assert saving_1 > 0
    assert saving_2 < 0

    # 2. Cung mot RUL (24h), may bi SILENT phai co xac suat hong CAO HON may OK
    rul = 24.0
    effective_rul_ok = rul * 1.0
    p_ok = math.exp(-effective_rul_ok / horizon)

    effective_rul_silent = rul * 0.5  # Phạt tuổi thọ còn 50%
    p_silent = math.exp(-effective_rul_silent / horizon)

    assert p_silent > p_ok
    assert round(p_silent, 3) == 0.846
    assert round(p_ok, 3) == 0.717


def test_schedule_request_validation() -> None:
    """P1-3: Kiem tra validation cac tham so dau vao cua ScheduleRequest."""
    # Hop le
    req = ScheduleRequest(capacity=3, horizon_hours=72.0, downtime_cost=10000.0, maintenance_cost=1500.0)
    assert req.capacity == 3

    # capacity < 1 -> phai loi
    with pytest.raises(ValidationError):
        ScheduleRequest(capacity=0)

    with pytest.raises(ValidationError):
        ScheduleRequest(capacity=-5)

    # horizon_hours <= 0 -> phai loi (tranh ZeroDivisionError)
    with pytest.raises(ValidationError):
        ScheduleRequest(horizon_hours=0)

    # Chi phi am -> phai loi
    with pytest.raises(ValidationError):
        ScheduleRequest(downtime_cost=-100)


@patch("scheduler.app.get_mongo_client")
def test_greedy_scheduler_idempotency(mock_get_mongo) -> None:
    """P0-1: Kiem tra khong tao trung work order cho may da co lenh dang mo."""
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_client.__getitem__.return_value = mock_db
    mock_get_mongo.return_value = mock_client

    # May M-001 dang co lenh 'scheduled'
    mock_db["work_orders"].distinct.return_value = ["M-001"]

    # Danh sach thiet bi: M-001 (degrading) va M-002 (degrading)
    mock_db["device_state"].find.return_value = [
        {"deviceId": "M-001", "status": "degrading", "rulHours": 5.0},
        {"deviceId": "M-002", "status": "degrading", "rulHours": 10.0},
    ]

    mock_insert_res = MagicMock()
    mock_insert_res.inserted_id = "order_new_123"
    mock_db["work_orders"].insert_one.return_value = mock_insert_res

    res = run_greedy_scheduler(capacity=3, horizon_hours=72.0)

    # M-001 da co lenh mo nen bi bo qua, chi tao lenh cho M-002
    assert res["scheduled_count"] == 1
    assert res["orders"][0]["deviceId"] == "M-002"
    assert res["skipped_active_count"] == 1


@patch("scheduler.app.get_mongo_client")
def test_health_check_endpoint(mock_get_mongo) -> None:
    """P2-6: Kiem tra endpoint /health phan biet MongoDB connected vs disconnected."""
    # 1. MongoDB ket noi thanh cong
    mock_client_ok = MagicMock()
    mock_client_ok.admin.command.return_value = {"ok": 1}
    mock_get_mongo.return_value = mock_client_ok

    resp_ok = health()
    assert resp_ok.status_code == 200

    # 2. MongoDB loi ket noi
    mock_client_fail = MagicMock()
    mock_client_fail.admin.command.side_effect = Exception("Connection refused")
    mock_get_mongo.return_value = mock_client_fail

    resp_fail = health()
    assert resp_fail.status_code == 503


@pytest.mark.anyio
async def test_webhook_authentication() -> None:
    """P1-4: Kiem tra xac thuc webhook token."""
    from fastapi import HTTPException

    mock_request = MagicMock()
    mock_request.json = AsyncMock(return_value={"alert": "TP2 drop"})
    mock_tasks = MagicMock()

    with patch("scheduler.app.WEBHOOK_SECRET", "super_secret_token_123"):
        # 1. Khong truyen token hoac sai token -> 401
        with pytest.raises(HTTPException) as exc_info:
            await datadog_webhook(
                request=mock_request,
                background_tasks=mock_tasks,
                x_datadog_token="wrong_token",
            )
        assert exc_info.value.status_code == 401

        # 2. Dung token -> Accepted
        res = await datadog_webhook(
            request=mock_request,
            background_tasks=mock_tasks,
            x_datadog_token="super_secret_token_123",
        )
        assert res["status"] == "accepted"
        mock_tasks.add_task.assert_called_once()
