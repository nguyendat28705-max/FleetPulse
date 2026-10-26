"""Kiem tra trang thai live streaming cua FleetPulse trong MongoDB (Cloud hoac Local)."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Dam bao UTF-8
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from mongo.client import (
    create_mongo_client,
    get_mongo_database_name,
    get_mongo_mode,
    get_mongo_uri,
    sanitize_uri,
)

load_dotenv()


def main() -> None:
    mode = get_mongo_mode()
    uri = get_mongo_uri()
    db_name = get_mongo_database_name()

    print("=" * 60)
    print(" FLEETPULSE - LIVE STATUS MONITOR")
    print("=" * 60)
    print(f"[*] MongoDB Mode    : {mode.upper()}")
    print(f"[*] Connection URI  : {sanitize_uri(uri)}")
    print(f"[*] Database Name   : {db_name}")
    print("-" * 60)

    try:
        client = create_mongo_client(uri, timeout_ms=6000)
        # Test ping
        client.admin.command("ping")
        print("[OK] Ket noi MongoDB thanh cong!")
        db = client[db_name]

        collections = db.list_collection_names()
        print(f"[*] Danh sach collections hien co: {collections}")

        # Thong ke tung collection
        col_names = ["telemetry", "features_live", "device_state", "work_orders", "fault_labels"]
        print("-" * 60)
        for col_name in col_names:
            if col_name in collections:
                count = db[col_name].count_documents({})
                print(f"  - {col_name:<16}: {count:>8} ban ghi")
            else:
                print(f"  - {col_name:<16}: (chua co)")

        print("-" * 60)

        # Hien thi mau du lieu device_state gan nhat
        if "device_state" in collections:
            sample_states = list(db["device_state"].find().sort("lastSeen", -1).limit(5))
            if sample_states:
                print("[*] Top 5 thiet bi cap nhat trang thai gan nhat:")
                for s in sample_states:
                    d_id = s.get("deviceId", "N/A")
                    hi = s.get("healthIndex", 1.0)
                    rul = s.get("rulHours", 999.0)
                    status = s.get("status", "ok")
                    last_seen = s.get("lastSeen", "N/A")
                    print(f"    - {d_id}: status={status:<10} | HI={hi:.3f} | RUL={rul:.1f}h | lastSeen={last_seen}")
            else:
                print("[!] Chua co ban ghi nao trong device_state.")

        # Hien thi mau du lieu features_live
        if "features_live" in collections:
            sample_feat = list(db["features_live"].find().sort("createdAt", -1).limit(2))
            if sample_feat:
                print("[*] 2 ban ghi features_live moi nhat:")
                for f in sample_feat:
                    d_id = f.get("deviceId", "N/A")
                    w_start = f.get("windowStart", "N/A")
                    shard = f.get("shard", "N/A")
                    f_data = f.get("feat", {}) or f.get("features", {})
                    tp2_mean = f_data.get("tp2_mean", "N/A")
                    tp3_mean = f_data.get("tp3_mean", "N/A")
                    h1_mean = f_data.get("h1_mean", "N/A")
                    print(f"    - {d_id} (shard {shard}) @ {w_start}: tp2_mean={tp2_mean}, tp3_mean={tp3_mean}, h1_mean={h1_mean}")

        print("=" * 60)

    except Exception as exc:
        print(f"[!] Loi khi ket noi hoac doc MongoDB: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
