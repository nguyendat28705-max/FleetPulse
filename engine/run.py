"""Động cơ streaming: đọc features_live qua Change Streams, tính HI, RUL, phát hiện silent failure, cập nhật device_state."""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from fp_algo.detect import Cusum
from fp_algo.health import HealthIndex
from fp_algo.rul import KalmanRul

# Tải cấu hình .env
load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI") or "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
DB_NAME = os.getenv("MONGODB_DATABASE") or os.getenv("MONGO_DB") or "fleetpulse_dev"
N_SHARDS = int(os.getenv("N_SHARDS") or "4")


class DeviceProcessor:
    """Theo dõi trạng thái và chạy thuật toán phân tích cho 1 thiết bị."""

    def __init__(self, device_id: str) -> None:
        self.device_id = device_id
        # Khởi tạo baseline tạm thời
        baseline_default = {
            "mean": [1.415, 3.28, 0.42, 0.07, 38.0],
            "inv_cov": [
                [1.0, 0.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 10.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 10.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.1],
            ],
        }
        self.health_calc = HealthIndex(baseline_default)
        self.kalman_rul = KalmanRul(hi_failure_threshold=6.0)
        self.cusum = Cusum(mu=1.0, sigma=0.5, k=0.5, h=3.0)

        self.last_seen_wall = time.time()
        self.status = "ok"
        self.hi = 1.0
        self.rul_hours = 120.0
        self.rul_low = 90.0
        self.rul_high = 150.0

    def process_features(self, feat_doc: dict[str, Any]) -> dict[str, Any]:
        """Xử lý một cửa sổ đặc trưng mới."""
        self.last_seen_wall = time.time()
        now_ts = time.time()

        feat = feat_doc.get("feat", {})
        self.hi = self.health_calc.update(feat)

        # Chạy CUSUM trên HI để phát hiện suy giảm chậm
        is_cusum_alert = self.cusum.update(self.hi)

        # Cập nhật RUL qua Kalman
        rul_info = self.kalman_rul.update(self.hi, now_ts)
        self.rul_hours = rul_info["rul_hours"]
        self.rul_low = rul_info["rul_low"]
        self.rul_high = rul_info["rul_high"]

        # Đánh giá trạng thái
        if is_cusum_alert or self.hi >= 3.5:
            self.status = "degrading"
        else:
            self.status = "ok"

        return {
            "deviceId": self.device_id,
            "status": self.status,
            "hi": self.hi,
            "rulHours": self.rul_hours,
            "rulHoursLow": self.rul_low,
            "rulHoursHigh": self.rul_high,
            "lastSeen": datetime.now(timezone.utc),
            "engineTs": datetime.now(timezone.utc).isoformat(),
        }

    def process_silence(self) -> dict[str, Any] | None:
        """Xử lý khi thiết bị bị mất kết nối / im lặng (Silent failure)."""
        now = time.time()
        silence_duration = now - self.last_seen_wall

        # Nếu quá 25 giây không có dữ liệu -> đánh dấu silent
        if silence_duration > 25.0:
            self.status = "silent"
            # Chạy bước dự báo thuần túy của Kalman (phương sai tăng dần)
            rul_info = self.kalman_rul.predict_only(now)
            self.rul_hours = rul_info["rul_hours"]
            self.rul_low = rul_info["rul_low"]
            self.rul_high = rul_info["rul_high"]

            return {
                "deviceId": self.device_id,
                "status": self.status,
                "hi": self.hi,
                "rulHours": self.rul_hours,
                "rulHoursLow": self.rul_low,
                "rulHoursHigh": self.rul_high,
                "lastSeen": datetime.now(timezone.utc),
                "engineTs": datetime.now(timezone.utc).isoformat(),
            }
        return None


class StreamingEngine:
    """Worker Engine đọc Change Stream từ collection features_live."""

    def __init__(
        self,
        shard_id: int = 0,
        total_shards: int = N_SHARDS,
        mongo_uri: str = MONGO_URI,
        db_name: str = DB_NAME,
    ) -> None:
        self.shard_id = shard_id
        self.total_shards = total_shards
        self.mongo_uri = mongo_uri
        self.db_name = db_name

        self.mongo_client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
        self.db = self.mongo_client[self.db_name]
        self.features_col = self.db["features_live"]
        self.state_col = self.db["device_state"]
        self.incidents_col = self.db["incidents"]

        self.processors: dict[str, DeviceProcessor] = {}
        self.running = True

    def _sync_state(self, state_doc: dict[str, Any]) -> None:
        """Cập nhật device_state và mở sự cố nếu suy giảm."""
        d_id = state_doc["deviceId"]
        self.state_col.update_one({"deviceId": d_id}, {"$set": state_doc}, upsert=True)

        if state_doc["status"] in ("degrading", "silent"):
            # Mở hoặc cập nhật incident
            incident = {
                "deviceId": d_id,
                "status": "active",
                "severity": "critical" if state_doc["status"] == "silent" else "high",
                "hi": state_doc["hi"],
                "rulHours": state_doc["rulHours"],
                "evidence": f"Trạng thái {state_doc['status']} với HI={state_doc['hi']}, RUL={state_doc['rulHours']}h",
                "updatedAt": datetime.now(timezone.utc),
            }
            self.incidents_col.update_one(
                {"deviceId": d_id, "status": "active"},
                {"$set": incident, "$setOnInsert": {"openedAt": datetime.now(timezone.utc)}},
                upsert=True,
            )

    def _silence_monitor_loop(self) -> None:
        """Thread kiểm tra silent failure định kỳ."""
        while self.running:
            time.sleep(5.0)
            for proc in list(self.processors.values()):
                state_doc = proc.process_silence()
                if state_doc:
                    self._sync_state(state_doc)

    def run(self) -> None:
        print(f"[*] Khởi động Streaming Engine cho Shard {self.shard_id}/{self.total_shards}...")

        # Chạy silence monitor
        t_silence = threading.Thread(target=self._silence_monitor_loop, daemon=True)
        t_silence.start()

        # Pipeline Change Stream lọc theo shard
        pipeline = [
            {
                "$match": {
                    "operationType": {"$in": ["insert", "replace", "update"]},
                    "$or": [
                        {"fullDocument.shard": self.shard_id},
                        {"fullDocument.shard": {"$exists": False}},  # phòng khi không có shard
                    ],
                }
            }
        ]

        try:
            with self.features_col.watch(pipeline=pipeline, full_document="updateLookup") as stream:
                print(f"[OK] Đang theo dõi Change Stream trên features_live (shard={self.shard_id})...")
                for change in stream:
                    if not self.running:
                        break

                    doc = change.get("fullDocument")
                    if not doc:
                        continue

                    d_id = doc.get("deviceId")
                    if not d_id:
                        continue

                    if d_id not in self.processors:
                        self.processors[d_id] = DeviceProcessor(d_id)

                    proc = self.processors[d_id]
                    state = proc.process_features(doc)
                    self._sync_state(state)

        except PyMongoError as e:
            print(f"[!] Lỗi Change Stream: {e}")
        except KeyboardInterrupt:
            print("\n[*] Dừng Engine...")
        finally:
            self.running = False
            self.mongo_client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="FleetPulse Streaming Engine Worker")
    parser.add_argument("--shard", type=int, default=0, help="Shard ID (0 -> shards-1)")
    parser.add_argument("--shards", type=int, default=N_SHARDS, help="Tổng số shard")
    parser.add_argument("--mongo-uri", default=MONGO_URI, help="MongoDB connection URI")
    parser.add_argument("--db", default=DB_NAME, help="Database name")
    args = parser.parse_args()

    engine = StreamingEngine(
        shard_id=args.shard,
        total_shards=args.shards,
        mongo_uri=args.mongo_uri,
        db_name=args.db,
    )
    engine.run()


if __name__ == "__main__":
    main()
