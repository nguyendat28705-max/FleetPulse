"""Cong cu tiem loi ro khi va cac su co gia dinh cho FleetPulse."""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timezone
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
from pymongo.errors import PyMongoError

from mongo.client import (
    create_mongo_client,
    get_mongo_database_name,
    get_mongo_uri,
    sanitize_uri,
)

# Load .env
load_dotenv()

MONGO_URI = get_mongo_uri()
DB_NAME = get_mongo_database_name()
VALID_FAULT_TYPES = {"air_leak", "sensor_drift", "stuck_comp"}


def validate_input(device_id: str, fault_type: str, severity: float, offset_sec: int) -> None:
    """Kiem tra tinh hop le cua du lieu dau vao (Validation at boundary)."""
    # 1. Kiem tra dinh dang device_id
    if not isinstance(device_id, str) or not re.match(r"^M-\d{3}$", device_id.strip()):
        raise ValueError(
            f"Ma thiet bi '{device_id}' khong hop le. Dinh dang chuan phai la M-XXX (vi du: M-001, M-017)."
        )

    fleet_size = int(os.getenv("FLEET_SIZE", "20"))
    dev_num = int(device_id.strip().split("-")[1])
    if dev_num < 1 or dev_num > max(fleet_size, 100):
        raise ValueError(
            f"Ma thiet bi '{device_id}' vuot ngoai pham vi doi tau cho phep (1 den {max(fleet_size, 100)})."
        )

    # 2. Kiem tra loai loi
    if fault_type not in VALID_FAULT_TYPES:
        raise ValueError(
            f"Loai loi '{fault_type}' khong duoc ho tro. Cac loai cho phep: {sorted(VALID_FAULT_TYPES)}."
        )

    # 3. Kiem tra mien gia tri severity
    if not (0.1 <= float(severity) <= 1.0):
        raise ValueError(
            f"Severity phai nam trong khoang [0.1, 1.0], nhan duoc: {severity}."
        )

    # 4. Kiem tra offset_sec
    if int(offset_sec) < 0:
        raise ValueError(
            f"offset_sec phai la so nguyen khong am (>= 0), nhan duoc: {offset_sec}."
        )


def inject_fault(
    device_id: str,
    fault_type: str,
    severity: float,
    offset_sec: int = 0,
    overwrite: bool = True,
) -> dict[str, Any]:
    """Tiem mot loi vao collection fault_labels trong MongoDB."""
    device_id = device_id.strip()
    validate_input(device_id, fault_type, severity, offset_sec)

    try:
        client = create_mongo_client(MONGO_URI)
    except Exception as exc:
        raise ConnectionError(f"Khong the ket noi toi MongoDB tai {sanitize_uri(MONGO_URI)}: {exc}") from exc

    try:
        db = client[DB_NAME]
        now = datetime.now(timezone.utc)

        # Kiem tra va giai quyet cac loi cu dang active cua cung thiet bi de tranh xung dot
        existing_active = list(db["fault_labels"].find({"deviceId": device_id, "resolved": {"$ne": True}}))
        if existing_active:
            if overwrite:
                update_res = db["fault_labels"].update_many(
                    {"deviceId": device_id, "resolved": {"$ne": True}},
                    {
                        "$set": {
                            "resolved": True,
                            "resolvedAt": now,
                            "resolvedReason": "overwritten_by_new_fault",
                        }
                    },
                )
                print(f"[*] Da dong {update_res.modified_count} loi cu dang hoat dong cua {device_id} truoc khi tiem loi moi.")
            else:
                raise RuntimeError(
                    f"Thiet bi {device_id} da co {len(existing_active)} loi chua resolved. Su dung overwrite=True de ghi de."
                )

        # Tinh toan thoi diem bat dau co hieu luc
        active_from_ts = now.timestamp() + int(offset_sec)
        active_from = datetime.fromtimestamp(active_from_ts, tz=timezone.utc)

        record: dict[str, Any] = {
            "deviceId": device_id,
            "type": fault_type,
            "severity": round(float(severity), 3),
            "t0": now,
            "offset_sec": int(offset_sec),
            "active_from": active_from,
            "createdAt": now,
            "resolved": False,
        }

        res = db["fault_labels"].insert_one(record)
        record["_id"] = str(res.inserted_id)

        print(
            f"[OK] Da tiem loi {fault_type} (severity={severity}, offset={offset_sec}s) cho {device_id} "
            f"luc {now.isoformat()} tren {sanitize_uri(MONGO_URI)}"
        )
        return record

    except PyMongoError as pme:
        raise RuntimeError(f"Loi truy van MongoDB trong qua trinh ghi fault_labels: {pme}") from pme
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="FleetPulse Fault Injector")
    parser.add_argument("--device", default="M-017", help="Ma thiet bi (vi du: M-017)")
    parser.add_argument("--type", default="air_leak", choices=sorted(VALID_FAULT_TYPES), help="Loai loi")
    parser.add_argument("--severity", type=float, default=0.6, help="Muc do nghiem trong (0.1 -> 1.0)")
    parser.add_argument("--offset-sec", type=int, default=0, help="Do lech thoi gian bat dau kich hoat loi (giay, >= 0)")
    parser.add_argument("--no-overwrite", action="store_true", help="Khong cho phep ghi de neu thiet bi dang co loi active")
    args = parser.parse_args()

    try:
        inject_fault(
            device_id=args.device,
            fault_type=args.type,
            severity=args.severity,
            offset_sec=args.offset_sec,
            overwrite=not args.no_overwrite,
        )
    except Exception as exc:
        print(f"[!] Loi thuc thi inject_fault: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
