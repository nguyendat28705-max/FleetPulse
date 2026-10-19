"""Scheduler and Action Service (FastAPI): Bộ lập lịch bảo trì có ràng buộc và Webhook."""

from __future__ import annotations

import math
import os
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, Request
from pydantic import BaseModel
from pymongo import MongoClient

# Tải cấu hình .env
load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI") or "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
DB_NAME = os.getenv("MONGODB_DATABASE") or os.getenv("MONGO_DB") or "fleetpulse_dev"

app = FastAPI(title="FleetPulse Action & Scheduler Service", version="1.0.0")


class ScheduleRequest(BaseModel):
    capacity: int = 3  # Số suất bảo trì tối đa mỗi ngày
    horizon_hours: float = 72.0  # Chân trời xem xét (giờ)
    downtime_cost: float = 10000.0  # Chi phí giả định khi dừng tàu do hỏng
    maintenance_cost: float = 1500.0  # Chi phí bảo trì chủ động


def get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)


def run_greedy_scheduler(
    capacity: int = 3,
    horizon_hours: float = 72.0,
    downtime_cost: float = 10000.0,
    maintenance_cost: float = 1500.0,
) -> dict[str, Any]:
    """Thuật toán Greedy chọn tối đa C máy để cực đại hoá (p_i * c_i - m_i)."""
    client = get_mongo_client()
    try:
        db = client[DB_NAME]
        states = list(db["device_state"].find())

        scored_candidates = []
        for s in states:
            d_id = s.get("deviceId")
            rul = float(s.get("rulHours", 999.0))
            status = s.get("status", "ok")

            # Tính xác suất hỏng trong chân trời horizon_hours
            if status == "silent":
                # Máy bị che khuất / im lặng -> rủi ro tăng cao
                prob_fail = min(0.95, math.exp(-max(1.0, rul) / (horizon_hours * 0.5)))
            else:
                prob_fail = min(0.95, math.exp(-max(1.0, rul) / horizon_hours))

            # Lợi ích kỳ vọng khi sửa máy i: p_i * c_i - m_i
            expected_saving = (prob_fail * downtime_cost) - maintenance_cost

            scored_candidates.append(
                {
                    "deviceId": d_id,
                    "status": status,
                    "rulHours": rul,
                    "prob_fail": round(prob_fail, 3),
                    "expected_saving": round(expected_saving, 2),
                }
            )

        # Sắp xếp giảm dần theo lợi ích kỳ vọng
        scored_candidates.sort(key=lambda x: x["expected_saving"], reverse=True)

        # Chọn top C máy có lợi ích > 0 hoặc trạng thái nguy cấp
        selected = [c for c in scored_candidates if c["expected_saving"] > 0 or c["status"] in ("degrading", "silent")][:capacity]

        # Ghi các lệnh bảo trì vào work_orders
        created_orders = []
        now = datetime.now(timezone.utc)
        for rank, item in enumerate(selected, start=1):
            order = {
                "deviceId": item["deviceId"],
                "priority_rank": rank,
                "reason": f"Status: {item['status']}, RUL: {item['rulHours']}h, Expected Saving: ${item['expected_saving']}",
                "status": "scheduled",
                "scheduledDate": now.isoformat(),
                "createdAt": now,
            }
            res = db["work_orders"].insert_one(order)
            order["_id"] = str(res.inserted_id)
            order["createdAt"] = order["createdAt"].isoformat()
            created_orders.append(order)

        return {
            "status": "success",
            "capacity": capacity,
            "scheduled_count": len(created_orders),
            "orders": created_orders,
        }
    finally:
        client.close()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "fleetpulse-scheduler"}


@app.post("/schedule")
def trigger_schedule(req: ScheduleRequest) -> dict[str, Any]:
    return run_greedy_scheduler(
        capacity=req.capacity,
        horizon_hours=req.horizon_hours,
        downtime_cost=req.downtime_cost,
        maintenance_cost=req.maintenance_cost,
    )


@app.post("/webhook")
async def datadog_webhook(request: Request, background_tasks: BackgroundTasks) -> dict[str, Any]:
    """Nhận alert webhook từ Datadog và tự động lập lịch bảo trì ưu tiên."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    print(f"[!] Webhook nhận tín hiệu cảnh báo từ Datadog: {body}")
    # Chạy lập lịch lại trong nền
    background_tasks.add_task(run_greedy_scheduler)
    return {"status": "accepted", "message": "Rescheduling triggered"}


@app.get("/work_orders")
def get_work_orders(limit: int = 20) -> list[dict[str, Any]]:
    client = get_mongo_client()
    try:
        db = client[DB_NAME]
        orders = list(db["work_orders"].find().sort("createdAt", -1).limit(limit))
        for o in orders:
            o["_id"] = str(o["_id"])
            if isinstance(o.get("createdAt"), datetime):
                o["createdAt"] = o["createdAt"].isoformat()
        return orders
    finally:
        client.close()
