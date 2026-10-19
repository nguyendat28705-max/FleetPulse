"""Ingestion Service: Lắng nghe MQTT, ghi telemetry (Time Series) và upsert features_live."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import zlib
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

# Tải cấu hình .env
load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI") or "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
DB_NAME = os.getenv("MONGODB_DATABASE") or os.getenv("MONGO_DB") or "fleetpulse_dev"
MQTT_HOST = os.getenv("MQTT_HOST") or "localhost"
MQTT_PORT = int(os.getenv("MQTT_HOST_PORT") or os.getenv("MQTT_PORT") or 1884)
N_SHARDS = int(os.getenv("N_SHARDS") or "4")


def parse_iso_or_now(val: Any) -> datetime:
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            pass
    return datetime.now(timezone.utc)


class IngestionService:
    """Dịch vụ nạp dữ liệu IoT từ MQTT vào MongoDB."""

    def __init__(
        self,
        mongo_uri: str = MONGO_URI,
        db_name: str = DB_NAME,
        host: str = MQTT_HOST,
        port: int = MQTT_PORT,
        n_shards: int = N_SHARDS,
    ) -> None:
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.host = host
        self.port = port
        self.n_shards = max(1, n_shards)

        self.mongo_client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
        self.db = self.mongo_client[self.db_name]

        self.telemetry_col = self.db["telemetry"]
        self.features_col = self.db["features_live"]

        # Bộ đệm ghi telemetry theo lô
        self.telemetry_batch: list[dict[str, Any]] = []
        self.last_flush = time.time()

        # MQTT Client
        self.client = mqtt.Client(client_id="fleetpulse_ingestion", callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.is_connected = False

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"[OK] Ingestion kết nối MQTT {self.host}:{self.port} thành công.")
            self.is_connected = True
            client.subscribe("fleet/+/+/telemetry")
            client.subscribe("fleet/+/+/features")
        else:
            print(f"[!] Ingestion kết nối MQTT thất bại: {rc}")

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            if topic.endswith("/telemetry"):
                self._handle_telemetry(payload)
            elif topic.endswith("/features"):
                self._handle_features(payload)
        except Exception as e:
            print(f"[!] Lỗi parse message ({topic}): {e}")

    def _handle_telemetry(self, p: dict[str, Any]) -> None:
        device_id = p.get("deviceId")
        if not device_id:
            return

        ts = parse_iso_or_now(p.get("ts"))
        vals = p.get("values", {})

        doc = {
            "ts": ts,
            "meta": {
                "deviceId": device_id,
                "line": p.get("line", "L1"),
                "gatewayId": p.get("gatewayId", "GW-01"),
            },
            "seq": p.get("seq", 0),
            "emit_ts": p.get("emit_ts"),
            "ingest_ts": datetime.now(timezone.utc).isoformat(),
        }
        # Thêm các cảm biến thô
        doc.update(vals)
        self.telemetry_batch.append(doc)

        # Xả lô nếu đã gom đủ 100 bản hoặc quá 1.0 giây
        if len(self.telemetry_batch) >= 100 or (time.time() - self.last_flush) > 1.0:
            self.flush_telemetry()

    def _handle_features(self, p: dict[str, Any]) -> None:
        device_id = p.get("deviceId")
        w_start = p.get("windowStart")
        if not device_id or not w_start:
            return

        # Gắn shard ổn định bằng crc32 (không dùng hash() của Python)
        shard_id = zlib.crc32(device_id.encode("utf-8")) % self.n_shards

        now_utc = datetime.now(timezone.utc)
        ts_chain = p.get("ts_chain", {})
        ts_chain["ingest_ts"] = now_utc.isoformat()

        doc = {
            "deviceId": device_id,
            "line": p.get("line", "L1"),
            "gatewayId": p.get("gatewayId", "GW-01"),
            "windowStart": str(w_start),
            "windowSec": p.get("windowSec", 10),
            "n": p.get("n", 0),
            "late": p.get("late", False),
            "feat": p.get("feat", {}),
            "edge": p.get("edge", {}),
            "shard": shard_id,
            "ts_chain": ts_chain,
            "createdAt": now_utc,
        }

        # Idempotent Upsert theo (deviceId, windowStart)
        self.features_col.update_one(
            {"deviceId": device_id, "windowStart": str(w_start)},
            {"$set": doc},
            upsert=True,
        )

    def flush_telemetry(self) -> None:
        if not self.telemetry_batch:
            return

        batch = self.telemetry_batch
        self.telemetry_batch = []
        self.last_flush = time.time()

        try:
            self.telemetry_col.insert_many(batch, ordered=False)
        except PyMongoError as e:
            print(f"[!] Lỗi chèn telemetry batch: {e}")

    def run(self) -> None:
        print("[*] Khởi động Ingestion Service...")
        self.client.connect(self.host, self.port, keepalive=60)
        self.client.loop_start()

        try:
            while True:
                time.sleep(1.0)
                if (time.time() - self.last_flush) > 1.0:
                    self.flush_telemetry()
        except KeyboardInterrupt:
            print("\n[*] Dừng Ingestion Service...")
        finally:
            self.flush_telemetry()
            self.client.loop_stop()
            self.client.disconnect()
            self.mongo_client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="FleetPulse Ingestion Service")
    parser.add_argument("--mongo-uri", default=MONGO_URI, help="MongoDB connection URI")
    parser.add_argument("--db", default=DB_NAME, help="Database name")
    parser.add_argument("--host", default=MQTT_HOST, help="MQTT host")
    parser.add_argument("--port", type=int, default=MQTT_PORT, help="MQTT port")
    parser.add_argument("--shards", type=int, default=N_SHARDS, help="Số shard cho engine")
    args = parser.parse_args()

    ingest = IngestionService(
        mongo_uri=args.mongo_uri,
        db_name=args.db,
        host=args.host,
        port=args.port,
        n_shards=args.shards,
    )
    ingest.run()


if __name__ == "__main__":
    main()
