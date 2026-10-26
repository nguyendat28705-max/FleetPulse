"""Script di chuyển dữ liệu MongoDB Local (Docker) sang MongoDB Cloud (Atlas) và ngược lại (Rollback).

Tính năng:
- Idempotent: chạy nhiều lần an toàn nhờ Bulk Upsert theo _id.
- Bảo toàn Index, Unique Constraints và TTL.
- Xác thực số lượng document trước và sau di chuyển.
- Che mật khẩu kết nối trên console/log.
- Hỗ trợ Rollback chiều ngược lại với cờ --reverse.

Cách dùng:
  # Di chuyển Local -> Cloud:
  python scripts/migrate_mongo.py --source-uri "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true" --target-uri "mongodb+srv://..."

  # Rollback Cloud -> Local:
  python scripts/migrate_mongo.py --reverse
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any

# Đảm bảo UTF-8 an toàn trên Windows console
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv
from pymongo import MongoClient, ReplaceOne, UpdateOne
from pymongo.errors import PyMongoError

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mongo.client import (
    DEFAULT_LOCAL_URI,
    get_mongo_database_name,
    get_mongo_uri,
    sanitize_uri,
)

load_dotenv()

CORE_COLLECTIONS = [
    "device_state",
    "features_live",
    "incidents",
    "work_orders",
    "fault_labels",
    "fleet_rollup_1m",
    "experiments",
    "telemetry",
]


def ensure_indexes_on_target(target_db: Any) -> None:
    """Tạo các index cần thiết trên target DB giống cấu trúc của mongo/setup.js."""
    print("[*] Đang đảm bảo cấu trúc collection và index trên Target Database...")

    existing_cols = target_db.list_collection_names()

    # 1. telemetry (Time Series nếu target hỗ trợ, hoặc regular collection)
    if "telemetry" not in existing_cols:
        try:
            target_db.create_collection(
                "telemetry",
                timeseries={"timeField": "ts", "metaField": "meta", "granularity": "seconds"},
                expireAfterSeconds=60 * 60 * 24 * 7,
            )
            print("  + telemetry (Time Series tạo mới)")
        except PyMongoError as e:
            # Một số tier hoặc standalone có thể không cho tạo timeseries với quyền hạn chế
            target_db.create_collection("telemetry")
            print(f"  + telemetry (Collection thông thường do: {e})")
    try:
        target_db.telemetry.create_index([("meta.deviceId", 1), ("ts", -1)])
    except Exception:
        pass

    # 2. features_live
    if "features_live" not in existing_cols:
        target_db.create_collection("features_live")
    target_db.features_live.create_index([("deviceId", 1), ("windowStart", 1)], unique=True)
    target_db.features_live.create_index([("createdAt", 1)], expireAfterSeconds=3600)
    target_db.features_live.create_index([("shard", 1)])

    # 3. device_state
    if "device_state" not in existing_cols:
        target_db.create_collection("device_state")
    target_db.device_state.create_index([("deviceId", 1)], unique=True)
    target_db.device_state.create_index([("status", 1), ("rulHoursLow", 1)])

    # 4. incidents & work_orders
    if "incidents" not in existing_cols:
        target_db.create_collection("incidents")
    target_db.incidents.create_index([("deviceId", 1), ("openedAt", -1)])
    target_db.incidents.create_index([("status", 1)])

    if "work_orders" not in existing_cols:
        target_db.create_collection("work_orders")
    target_db.work_orders.create_index([("deviceId", 1), ("createdAt", -1)])
    target_db.work_orders.create_index([("status", 1), ("scheduledDate", 1)])

    # 5. fleet_rollup_1m & fault_labels
    if "fleet_rollup_1m" not in existing_cols:
        target_db.create_collection("fleet_rollup_1m")
    target_db.fleet_rollup_1m.create_index([("minute", 1)], expireAfterSeconds=60 * 60 * 24 * 30)
    target_db.fleet_rollup_1m.create_index([("deviceId", 1), ("minute", -1)])

    if "fault_labels" not in existing_cols:
        target_db.create_collection("fault_labels")
    target_db.fault_labels.create_index([("deviceId", 1), ("t0", 1)])

    if "experiments" not in existing_cols:
        target_db.create_collection("experiments")
    target_db.experiments.create_index([("suite", 1), ("createdAt", -1)])
    print("[OK] Đã hoàn thành cấu trúc index trên Target Database.")


def migrate_collection(
    source_db: Any,
    target_db: Any,
    col_name: str,
    batch_size: int = 500,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Di chuyển một collection theo lô (idempotent upsert)."""
    if col_name not in source_db.list_collection_names():
        return {"collection": col_name, "source_count": 0, "migrated": 0, "status": "skipped_not_found"}

    src_col = source_db[col_name]
    tgt_col = target_db[col_name]

    source_count = src_col.count_documents({})
    if source_count == 0:
        return {"collection": col_name, "source_count": 0, "migrated": 0, "status": "empty"}

    print(f"\n[*] Đang di chuyển '{col_name}' ({source_count} bản ghi)...")
    if dry_run:
        print(f"  [DRY-RUN] Sẽ sao chép {source_count} documents sang Target.")
        return {"collection": col_name, "source_count": source_count, "migrated": 0, "status": "dry_run"}

    migrated = 0
    cursor = src_col.find({}, batch_size=batch_size)
    batch = []

    for doc in cursor:
        doc_id = doc.get("_id")
        if doc_id is not None:
            batch.append(ReplaceOne({"_id": doc_id}, doc, upsert=True))
        else:
            batch.append(UpdateOne(doc, {"$set": doc}, upsert=True))

        if len(batch) >= batch_size:
            tgt_col.bulk_write(batch, ordered=False)
            migrated += len(batch)
            print(f"  -> Tiến độ: {migrated}/{source_count} documents...")
            batch = []

    if batch:
        tgt_col.bulk_write(batch, ordered=False)
        migrated += len(batch)

    target_count = tgt_col.count_documents({})
    status = "verified" if target_count >= source_count else "partial"
    print(f"[OK] '{col_name}': Nguồn = {source_count}, Đích = {target_count} ({status})")

    return {
        "collection": col_name,
        "source_count": source_count,
        "target_count": target_count,
        "migrated": migrated,
        "status": status,
    }


def run_migration(
    source_uri: str,
    target_uri: str,
    db_name: str,
    collections: list[str] | None = None,
    batch_size: int = 500,
    dry_run: bool = False,
) -> bool:
    print("=" * 60)
    print("          FLEETPULSE MONGODB DATA MIGRATION TOOL          ")
    print("=" * 60)
    print(f"Source: {sanitize_uri(source_uri)}")
    print(f"Target: {sanitize_uri(target_uri)}")
    print(f"Database: {db_name}")
    print(f"Batch Size: {batch_size} | Dry Run: {dry_run}")
    print("-" * 60)

    try:
        src_client = MongoClient(source_uri, serverSelectionTimeoutMS=8000)
        tgt_client = MongoClient(target_uri, serverSelectionTimeoutMS=8000)

        # Kiểm tra kết nối ping
        src_client.admin.command("ping")
        print("[OK] Kết nối Source thành công.")
        tgt_client.admin.command("ping")
        print("[OK] Kết nối Target thành công.")

        src_db = src_client[db_name]
        tgt_db = tgt_client[db_name]

        if not dry_run:
            ensure_indexes_on_target(tgt_db)

        cols_to_migrate = collections or CORE_COLLECTIONS
        summary = []
        all_ok = True

        for col in cols_to_migrate:
            res = migrate_collection(src_db, tgt_db, col, batch_size=batch_size, dry_run=dry_run)
            summary.append(res)
            if res.get("status") not in ("verified", "empty", "skipped_not_found", "dry_run"):
                all_ok = False

        print("\n" + "=" * 60)
        print("KẾT QUẢ MIGRATION TỔNG HỢP:")
        for s in summary:
            print(f" - {s['collection']:<18}: Nguồn={s.get('source_count', 0):<6} | Đích={s.get('target_count', 0):<6} | Trạng thái: {s['status']}")
        print("=" * 60)
        return all_ok

    except PyMongoError as e:
        print(f"\n[!] LỖI KẾT NỐI MONGODB TRONG QUÁ TRÌNH MIGRATION: {e}")
        return False
    finally:
        try:
            src_client.close()
            tgt_client.close()
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="FleetPulse MongoDB Migration Tool")
    parser.add_argument("--source-uri", default=None, help="URI của MongoDB nguồn")
    parser.add_argument("--target-uri", default=None, help="URI của MongoDB đích")
    parser.add_argument("--db", default=get_mongo_database_name(), help="Tên database")
    parser.add_argument("--collections", nargs="*", default=None, help="Danh sách collection cần chuyển")
    parser.add_argument("--batch-size", type=int, default=500, help="Kích thước lô ghi")
    parser.add_argument("--dry-run", action="store_true", help="Chỉ kiểm tra, không ghi dữ liệu")
    parser.add_argument("--reverse", action="store_true", help="Chạy ngược chiều: Cloud -> Local (Rollback)")
    args = parser.parse_args()

    cloud_uri = get_mongo_uri("cloud")
    local_uri = DEFAULT_LOCAL_URI

    if args.reverse:
        src = args.source_uri or cloud_uri
        tgt = args.target_uri or local_uri
        print("[!] Chế độ ROLLBACK: Di chuyển từ Cloud về Local.")
    else:
        src = args.source_uri or local_uri
        tgt = args.target_uri or cloud_uri
        print("[*] Chế độ MIGRATION: Di chuyển từ Local lên Cloud.")

    success = run_migration(
        source_uri=src,
        target_uri=tgt,
        db_name=args.db,
        collections=args.collections,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
