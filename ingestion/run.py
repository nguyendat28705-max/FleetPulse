"""Ingestion Service: Lang nghe MQTT, ghi telemetry (Time Series) va upsert features_live."""

from __future__ import annotations

import argparse
import collections
import json
import math
import os
import sys
import time
import zlib
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
import paho.mqtt.client as mqtt
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, PyMongoError

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
MQTT_HOST = os.getenv("MQTT_HOST") or "localhost"
MQTT_PORT = int(os.getenv("MQTT_HOST_PORT") or os.getenv("MQTT_PORT") or 1883)
N_SHARDS = int(os.getenv("N_SHARDS") or "4")
BATCH_SIZE = int(os.getenv("INGESTION_BATCH_SIZE") or "100")
FLUSH_INTERVAL_SEC = float(os.getenv("INGESTION_FLUSH_INTERVAL") or "1.0")
MAX_BUFFER_RECORDS = int(os.getenv("INGESTION_MAX_BUFFER") or "10000")
DEDUP_CACHE_SIZE = int(os.getenv("INGESTION_DEDUP_CACHE_SIZE") or "50000")

# Danh sach cac cam bien tho cho phep (chong field pollution va ghi de metadata)
ALLOWED_SENSOR_KEYS = {
    "TP2",
    "TP3",
    "H1",
    "DV_pressure",
    "Reservoirs",
    "Motor_current",
    "Oil_temperature",
    "COMP",
    "DV_eletric",
    "Towers",
    "MPG",
    "LPS",
    "Pressure_switch",
    "Oil_level",
    "Caudal_impulses",
}


def parse_utc_iso(val: Any) -> datetime | None:
    """Parse chuoi ISO timestamp sang datetime co mui gio UTC.
    
    Tra ve None neu khong parse duoc hoac khong dung dinh dang, tranh am tham sua gio.
    """
    if not isinstance(val, str) or not val.strip():
        return None
    try:
        clean_str = val.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean_str)
        # Neu datetime khong co timezone info thi gan UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def format_canonical_iso(dt: datetime) -> str:
    """Chuan hoa datetime UTC ve chuoi ISO 8601 dong nhat (VD: 2026-10-24T05:00:00.000Z)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class IngestionService:
    """Dich vu nap du lieu IoT tu MQTT vao MongoDB."""

    def __init__(
        self,
        mongo_uri: str = MONGO_URI,
        db_name: str = DB_NAME,
        host: str = MQTT_HOST,
        port: int = MQTT_PORT,
        n_shards: int = N_SHARDS,
        batch_size: int = BATCH_SIZE,
        flush_interval: float = FLUSH_INTERVAL_SEC,
    ) -> None:
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.host = host
        self.port = port
        self.n_shards = max(1, n_shards)
        self.batch_size = max(1, batch_size)
        self.flush_interval = max(0.1, flush_interval)

        self.mongo_client = create_mongo_client(self.mongo_uri, timeout_ms=5000)
        self.db = self.mongo_client[self.db_name]

        self.telemetry_col = self.db["telemetry"]
        self.features_col = self.db["features_live"]
        self._ensure_indexes()

        # Bo dem ghi telemetry theo lo (micro-batching)
        self.telemetry_batch: list[dict[str, Any]] = []
        self.last_flush = time.time()

        # Bo dem chong ghi trung (In-memory Deduplication Cache)
        # Dung OrderedDict nhu mot LRU Set
        self.dedup_cache: collections.OrderedDict[str, None] = collections.OrderedDict()

        # Metrics van hanh
        self.messages_received = 0
        self.messages_invalid = 0
        self.duplicates_dropped = 0
        self.telemetry_inserted = 0
        self.telemetry_failed = 0
        self.features_upserted = 0
        self.features_failed = 0

        # MQTT Client
        self.client = mqtt.Client(
            client_id="fleetpulse_ingestion",
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.is_connected = False

    def _ensure_indexes(self) -> None:
        """Kiem tra va tao unique index dam bao idempotency."""
        try:
            # Unique compound index cho features_live
            self.features_col.create_index(
                [("deviceId", 1), ("windowStart", 1)],
                unique=True,
                background=True,
            )
        except Exception as exc:
            print(f"[WARN] Khong the khoi tao index tren features_live: {exc}")

    def _is_duplicate_telemetry(self, dedup_key: str) -> bool:
        """Kiem tra va luu dedup_key vao LRU cache."""
        if dedup_key in self.dedup_cache:
            return True
        self.dedup_cache[dedup_key] = None
        if len(self.dedup_cache) > DEDUP_CACHE_SIZE:
            self.dedup_cache.popitem(last=False)  # Xoa phan tu cu nhat
        return False

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"[OK] Ingestion ket noi MQTT {self.host}:{self.port} thanh cong.")
            self.is_connected = True
            client.subscribe("fleet/+/+/telemetry")
            client.subscribe("fleet/+/+/features")
        else:
            print(f"[!] Ingestion ket noi MQTT that bai: {rc}")

    def _on_disconnect(self, client, userdata, disconnect_flags, rc=None, properties=None):
        self.is_connected = False
        print("[!] Ingestion mat ket noi MQTT broker.")

    def _on_message(self, client, userdata, msg):
        self.messages_received += 1
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            if topic.endswith("/telemetry"):
                self._handle_telemetry(payload)
            elif topic.endswith("/features"):
                self._handle_features(payload)
        except Exception as e:
            self.messages_invalid += 1
            print(f"[!] Loi parse message ({topic}): {e}")

    def _handle_telemetry(self, p: dict[str, Any]) -> None:
        if not isinstance(p, dict):
            self.messages_invalid += 1
            return

        device_id = p.get("deviceId")
        if not device_id or not isinstance(device_id, str) or not device_id.strip():
            self.messages_invalid += 1
            return
        device_id = device_id.strip()

        # Parse timestamp nghiem ngat: uu tien emit_ts (live stream), roi den ts
        ts_dt = parse_utc_iso(p.get("emit_ts")) or parse_utc_iso(p.get("ts"))
        if ts_dt is None:
            # Khong parse duoc timestamp -> ghi log canh bao va drop ban ghi loi
            self.messages_invalid += 1
            return

        seq = int(p.get("seq", 0))
        sim_ts = p.get("ts")
        emit_ts = p.get("emit_ts")

        # Kiem tra chong ghi trung (Idempotency check)
        dedup_key = f"{device_id}:{seq}:{sim_ts or emit_ts}"
        if self._is_duplicate_telemetry(dedup_key):
            self.duplicates_dropped += 1
            return

        # Validate va trich xuat an toan values (chong ghi de ts, meta, seq)
        vals = p.get("values")
        clean_sensors: dict[str, Any] = {}
        if isinstance(vals, dict):
            for k in ALLOWED_SENSOR_KEYS:
                if k in vals:
                    v = vals[k]
                    # Kiem tra so huu han
                    if isinstance(v, (int, float)):
                        if not (math.isnan(v) or math.isinf(v)):
                            clean_sensors[k] = v

        doc = {
            "ts": ts_dt,
            "meta": {
                "deviceId": device_id,
                "line": str(p.get("line") or "L1"),
                "gatewayId": str(p.get("gatewayId") or "GW-01"),
            },
            "seq": seq,
            "sim_ts": str(sim_ts) if sim_ts else None,
            "emit_ts": str(emit_ts) if emit_ts else None,
            "ingest_ts": datetime.now(timezone.utc).isoformat(),
        }
        doc.update(clean_sensors)

        # Chặn tràn RAM nếu MongoDB mất kết nối dài hạn
        if len(self.telemetry_batch) < MAX_BUFFER_RECORDS:
            self.telemetry_batch.append(doc)
        else:
            self.telemetry_failed += 1
            print(f"[WARN] Bo dem Telemetry day ({MAX_BUFFER_RECORDS} ban ghi), loai bo bot ban tin de tranh tran RAM.")

        # Xa lo theo kich thuoc batch (Size-based flush)
        if len(self.telemetry_batch) >= self.batch_size:
            self.flush_telemetry()

    def _handle_features(self, p: dict[str, Any]) -> None:
        if not isinstance(p, dict):
            self.features_failed += 1
            return

        device_id = p.get("deviceId")
        raw_w_start = p.get("windowStart")
        if not device_id or not raw_w_start:
            self.features_failed += 1
            return
        device_id = str(device_id).strip()

        # Chuan hoa windowStart thanh Canonical ISO de tranh sai lech chuoi trong unique index
        w_start_dt = parse_utc_iso(raw_w_start)
        if w_start_dt is not None:
            w_start_str = format_canonical_iso(w_start_dt)
        else:
            w_start_str = str(raw_w_start).strip()

        # Application-level Logical Sharding: chia thiet bi thanh cac nhom logic cho Streaming Engine workers
        # (Luu y: Day khong phai Physical MongoDB Sharding tren cluster ma la Worker Partitioning)
        shard_id = zlib.crc32(device_id.encode("utf-8")) % self.n_shards

        now_utc = datetime.now(timezone.utc)

        # Kiem tra an toan cac dict long nhau (chong TypeError neu sender gui null)
        ts_chain = p.get("ts_chain") if isinstance(p.get("ts_chain"), dict) else {}
        ts_chain["ingest_ts"] = now_utc.isoformat()

        feat = p.get("feat") if isinstance(p.get("feat"), dict) else {}
        edge = p.get("edge") if isinstance(p.get("edge"), dict) else {}

        doc = {
            "deviceId": device_id,
            "line": str(p.get("line") or "L1"),
            "gatewayId": str(p.get("gatewayId") or "GW-01"),
            "windowStart": w_start_str,
            "windowSec": int(p.get("windowSec") or 10),
            "n": int(p.get("n") or 0),
            "late": bool(p.get("late", False)),
            "feat": feat,
            "edge": edge,
            "shard": shard_id,
            "ts_chain": ts_chain,
            "createdAt": now_utc,
        }

        try:
            # Idempotent Upsert theo (deviceId, windowStart)
            self.features_col.update_one(
                {"deviceId": device_id, "windowStart": w_start_str},
                {"$set": doc},
                upsert=True,
            )
            self.features_upserted += 1
        except Exception as exc:
            self.features_failed += 1
            print(f"[!] Loi upsert features_live ({device_id} @ {w_start_str}): {exc}")

    def flush_telemetry(self) -> None:
        """Thuc thi ghi lo telemetry vao MongoDB voi co che bao toan du lieu."""
        if not self.telemetry_batch:
            return

        # Lay danh sach hien tai va reset timestamp flush
        batch_to_send = list(self.telemetry_batch)
        self.last_flush = time.time()

        try:
            res = self.telemetry_col.insert_many(batch_to_send, ordered=False)
            inserted_count = len(res.inserted_ids)
            self.telemetry_inserted += inserted_count

            # Ghi thanh cong toan bo -> cat bo dung so luong do khoi telemetry_batch
            # (khong gan rong vi trong luc insert co the co message moi append vao)
            self.telemetry_batch = self.telemetry_batch[len(batch_to_send):]

        except BulkWriteError as bwe:
            # Xử lý trường hợp ghi một phần (partial write)
            details = bwe.details
            inserted_count = details.get("nInserted", 0)
            self.telemetry_inserted += inserted_count

            # Tim cac index bi loi
            write_errors = details.get("writeErrors", [])
            failed_indices = {err["index"] for err in write_errors if "index" in err}

            # Loc ra nhung ban ghi bi loi thuc su (khong phai duplicate code 11000)
            retry_docs = []
            for idx in failed_indices:
                err_detail = next((e for e in write_errors if e.get("index") == idx), {})
                # Neu loi khong phai duplicate key -> can giu lai de retry
                if err_detail.get("code") != 11000:
                    if idx < len(batch_to_send):
                        retry_docs.append(batch_to_send[idx])
                else:
                    self.duplicates_dropped += 1

            # Cap nhat lai batch: giu nhung ban ghi bi loi can retry + cac ban ghi moi den
            new_incoming = self.telemetry_batch[len(batch_to_send):]
            self.telemetry_batch = retry_docs + new_incoming
            self.telemetry_failed += len(failed_indices)
            print(f"[WARN] BulkWriteError telemetry: da chen {inserted_count}, loi {len(failed_indices)} ban ghi.")

        except PyMongoError as pme:
            # Loi ket noi MongoDB toan bo (khong co ban ghi nao duoc chen)
            # GIU NGUYEN batch de retry o chu ky ke tiep, tuyet doi khong lam mat du lieu
            self.telemetry_failed += len(batch_to_send)
            print(f"[!] Loi ket noi MongoDB khi chen telemetry batch ({len(batch_to_send)} ban ghi): {pme}")

    def run(self) -> None:
        print(f"[*] Khoi dong Ingestion Service (MongoDB: {sanitize_uri(self.mongo_uri)} -> {self.db_name})...")
        connected = False
        while not connected:
            try:
                self.client.connect(self.host, self.port, keepalive=60)
                connected = True
            except Exception as e:
                print(f"[!] Ingestion chua ket noi duoc MQTT ({e}). Thu lai sau 2 giay...")
                time.sleep(2.0)
        self.client.loop_start()

        try:
            while True:
                time.sleep(0.5)
                # Kiem tra flush theo thoi gian (Time-based flush)
                if (time.time() - self.last_flush) >= self.flush_interval:
                    self.flush_telemetry()
        except KeyboardInterrupt:
            print("\n[*] Dung Ingestion Service...")
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
    parser.add_argument("--shards", type=int, default=N_SHARDS, help="So shard cho engine")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Kich thuoc batch telemetry")
    args = parser.parse_args()

    ingest = IngestionService(
        mongo_uri=args.mongo_uri,
        db_name=args.db,
        host=args.host,
        port=args.port,
        n_shards=args.shards,
        batch_size=args.batch_size,
    )
    ingest.run()


if __name__ == "__main__":
    main()
