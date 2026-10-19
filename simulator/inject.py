"""Công cụ tiêm lỗi rò khí và các sự cố giả định cho FleetPulse."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient

# Load .env
load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI") or "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
DB_NAME = os.getenv("MONGODB_DATABASE") or os.getenv("MONGO_DB") or "fleetpulse_dev"


def inject_fault(device_id: str, fault_type: str, severity: float, offset_sec: int = 0) -> dict:
    """Tiêm một lỗi vào collection fault_labels trong MongoDB."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        db = client[DB_NAME]
        now = datetime.now(timezone.utc)
        record = {
            "deviceId": device_id,
            "type": fault_type,
            "severity": float(severity),
            "t0": now,
            "offset_sec": offset_sec,
            "createdAt": now,
            "resolved": False,
        }
        res = db["fault_labels"].insert_one(record)
        record["_id"] = str(res.inserted_id)
        print(f"[OK] Da tiem loi {fault_type} (do nghiem trong={severity}) cho {device_id} luc {now.isoformat()}")
        return record
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="FleetPulse Fault Injector")
    parser.add_argument("--device", default="M-017", help="Ma thiet bi (vi du: M-017)")
    parser.add_argument("--type", default="air_leak", choices=["air_leak", "sensor_drift", "stuck_comp"], help="Loai loi")
    parser.add_argument("--severity", type=float, default=0.6, help="Muc do nghiem trong (0.1 -> 1.0)")
    parser.add_argument("--offset-sec", type=int, default=0, help="Do lech thoi gian bat dau (giay)")
    args = parser.parse_args()

    inject_fault(args.device, args.type, args.severity, args.offset_sec)


if __name__ == "__main__":
    main()
