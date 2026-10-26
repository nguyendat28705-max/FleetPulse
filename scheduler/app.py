"""Scheduler and Action Service (FastAPI): Bo lap lich bao tri co rang buoc, Idempotent va Webhook xac thuc."""

from __future__ import annotations

import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Dam bao UTF-8 an toan tren Windows console
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from mongo.client import (
    create_mongo_client,
    get_mongo_database_name,
    get_mongo_uri,
    sanitize_uri,
)

# Tai cau hinh .env
load_dotenv()

MONGO_URI = get_mongo_uri()
DB_NAME = get_mongo_database_name()
WEBHOOK_SECRET = os.getenv("SCHEDULER_WEBHOOK_SECRET", "")

app = FastAPI(title="FleetPulse Action & Scheduler Service", version="1.1.0")


class ScheduleRequest(BaseModel):
    capacity: int = Field(default=3, ge=1, le=50, description="So suat bao tri toi da moi dot lap lich")
    horizon_hours: float = Field(default=72.0, gt=0.0, le=720.0, description="Chan troi xem xet rui ro (gio)")
    downtime_cost: float = Field(default=10000.0, ge=0.0, description="Chi phi gia dinh khi dung tau do hong")
    maintenance_cost: float = Field(default=1500.0, ge=0.0, description="Chi phi bao tri chu dong")
    force_reschedule: bool = Field(default=False, description="Cho phep lap lich lai ke ca khi thiet bi da co lenh mo")


def get_mongo_client() -> MongoClient:
    return create_mongo_client(MONGO_URI, timeout_ms=3000)


def ensure_indexes(db) -> None:
    """Dam bao index cho work_orders de ho tro chong trung va truy van nhanh."""
    try:
        db["work_orders"].create_index([("deviceId", 1), ("status", 1)], background=True)
        db["work_orders"].create_index([("createdAt", -1)], background=True)
        db["scheduler_runs"].create_index([("executedAt", -1)], background=True)
    except Exception as exc:
        print(f"[WARN] Khong the tao index cho work_orders: {exc}")


def run_greedy_scheduler(
    capacity: int = 3,
    horizon_hours: float = 72.0,
    downtime_cost: float = 10000.0,
    maintenance_cost: float = 1500.0,
    force_reschedule: bool = False,
) -> dict[str, Any]:
    """Thuat toan Greedy chon toi da C may co loi ich ky vong cao nhat, bao dam Idempotency."""
    client = get_mongo_client()
    now_utc = datetime.now(timezone.utc)
    run_audit: dict[str, Any] = {
        "executedAt": now_utc,
        "capacity": capacity,
        "horizon_hours": horizon_hours,
        "downtime_cost": downtime_cost,
        "maintenance_cost": maintenance_cost,
    }

    try:
        db = client[DB_NAME]
        ensure_indexes(db)

        # 1. Truy van cac thiet bi dang co lenh bao tri chua hoan thanh de CHONG TRUNG
        active_devices_with_orders = set()
        if not force_reschedule:
            active_devices_with_orders = set(
                db["work_orders"].distinct(
                    "deviceId",
                    {"status": {"$in": ["scheduled", "in_progress", "pending"]}},
                )
            )

        # 2. Doc trang thai cac thiet bi
        states = list(db["device_state"].find())
        scored_candidates = []

        for s in states:
            d_id = s.get("deviceId")
            if not d_id or not isinstance(d_id, str):
                continue
            d_id = d_id.strip()

            # Neu thiet bi da co lenh dang mo va khong force -> bo qua de tranh tao trung
            if d_id in active_devices_with_orders:
                continue

            try:
                raw_rul = s.get("rulHours", 999.0)
                rul = float(raw_rul)
                if math.isnan(rul) or math.isinf(rul):
                    rul = 999.0
            except (ValueError, TypeError):
                rul = 999.0

            status_str = str(s.get("status") or "ok").lower()

            # Tinh xac suat hong trong chan troi horizon_hours:
            # QUAN TRONG: Voi trang thai 'silent', thiet bi mat ket noi nen rui ro phai TANG LEN.
            # Ta phat bang cach giam RUL hieu dung xuong con 50% (effective_rul = rul * 0.5),
            # khi do exp(-effective_rul / horizon) se lon hon, lam prob_fail TANG LEN hop ly!
            effective_rul = max(1.0, rul) * (0.5 if status_str == "silent" else 1.0)
            prob_fail = min(0.95, math.exp(-effective_rul / horizon_hours))

            # Loi ich ky vong: p_i * c_i - m_i
            expected_saving = (prob_fail * downtime_cost) - maintenance_cost

            scored_candidates.append(
                {
                    "deviceId": d_id,
                    "status": status_str,
                    "rulHours": round(rul, 1),
                    "prob_fail": round(prob_fail, 3),
                    "expected_saving": round(expected_saving, 2),
                }
            )

        # Sap xep giam dan theo loi ich ky vong
        scored_candidates.sort(key=lambda x: x["expected_saving"], reverse=True)

        # Chon top C may co loi ich > 0 hoac dang o trang thai nguy cap (degrading/silent)
        selected = [
            c for c in scored_candidates
            if c["expected_saving"] > 0 or c["status"] in ("degrading", "silent")
        ][:capacity]

        # 3. Ghi cac lenh bao tri vao work_orders (dong nhat Date BSON UTC)
        created_orders = []
        scheduled_for = now_utc + timedelta(days=1)  # Du kien bao tri vao ngay mai

        for rank, item in enumerate(selected, start=1):
            order = {
                "deviceId": item["deviceId"],
                "priority_rank": rank,
                "reason": (
                    f"Status: {item['status']}, RUL: {item['rulHours']}h, "
                    f"ProbFail: {item['prob_fail']}, Expected Saving: ${item['expected_saving']}"
                ),
                "status": "scheduled",
                "scheduledDate": scheduled_for,
                "createdAt": now_utc,
            }
            res = db["work_orders"].insert_one(order)
            order["_id"] = str(res.inserted_id)
            # Format thanh chuoi ISO khi tra JSON qua API
            order["scheduledDate"] = scheduled_for.isoformat()
            order["createdAt"] = now_utc.isoformat()
            created_orders.append(order)

        result = {
            "status": "success",
            "capacity": capacity,
            "scheduled_count": len(created_orders),
            "skipped_active_count": len(active_devices_with_orders),
            "orders": created_orders,
        }

        # Ghi audit log lich su chay
        run_audit.update({
            "status": "success",
            "scheduled_count": len(created_orders),
            "created_order_ids": [o["_id"] for o in created_orders],
        })
        try:
            db["scheduler_runs"].insert_one(run_audit)
        except Exception:
            pass

        return result

    except Exception as exc:
        run_audit["status"] = "failed"
        run_audit["error"] = str(exc)
        try:
            db["scheduler_runs"].insert_one(run_audit)
        except Exception:
            pass
        raise
    finally:
        client.close()


@app.get("/health")
def health() -> JSONResponse:
    """Liveness & Readiness probe: kiem tra tien trinh va ket noi MongoDB."""
    mongo_ok = False
    error_msg = None
    try:
        client = get_mongo_client()
        client.admin.command("ping")
        mongo_ok = True
        client.close()
    except Exception as exc:
        error_msg = str(exc)

    resp_data = {
        "status": "ok" if mongo_ok else "degraded",
        "service": "fleetpulse-scheduler",
        "database": "connected" if mongo_ok else "disconnected",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if not mongo_ok:
        resp_data["error"] = error_msg
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=resp_data)
    return JSONResponse(status_code=status.HTTP_200_OK, content=resp_data)


@app.post("/schedule")
def trigger_schedule(req: ScheduleRequest) -> dict[str, Any]:
    return run_greedy_scheduler(
        capacity=req.capacity,
        horizon_hours=req.horizon_hours,
        downtime_cost=req.downtime_cost,
        maintenance_cost=req.maintenance_cost,
        force_reschedule=req.force_reschedule,
    )


@app.post("/webhook")
async def datadog_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_datadog_token: str | None = Header(default=None, alias="X-Datadog-Token"),
    x_webhook_secret: str | None = Header(default=None, alias="X-Webhook-Secret"),
) -> dict[str, Any]:
    """Nhan alert webhook tu Datadog va tu dong lap lich bao tri co xac thuc token."""
    # 1. Kiem tra xac thuc webhook neu co cau hinh secret
    if WEBHOOK_SECRET:
        token = x_datadog_token or x_webhook_secret or request.query_params.get("secret")
        if token != WEBHOOK_SECRET:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing webhook authentication token.",
            )

    try:
        body = await request.json()
    except Exception:
        body = {}

    print(f"[!] Webhook nhan tin hieu canh bao hop le tu Datadog: {body}")
    # Chay scheduler duoi nen voi cac tham so chuan
    background_tasks.add_task(run_greedy_scheduler, capacity=3, horizon_hours=72.0)
    return {
        "status": "accepted",
        "message": "Rescheduling triggered successfully in background",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/work_orders")
def get_work_orders(
    limit: int = Query(default=20, ge=1, le=100, description="So luong ban ghi toi da (1 - 100)"),
    status_filter: str | None = Query(default=None, description="Loc theo trang thai (scheduled/in_progress/completed)"),
) -> list[dict[str, Any]]:
    client = get_mongo_client()
    try:
        db = client[DB_NAME]
        query: dict[str, Any] = {}
        if status_filter:
            query["status"] = status_filter

        orders = list(db["work_orders"].find(query).sort("createdAt", -1).limit(limit))
        for o in orders:
            o["_id"] = str(o["_id"])
            if isinstance(o.get("createdAt"), datetime):
                o["createdAt"] = o["createdAt"].isoformat()
            if isinstance(o.get("scheduledDate"), datetime):
                o["scheduledDate"] = o["scheduledDate"].isoformat()
        return orders
    finally:
        client.close()
