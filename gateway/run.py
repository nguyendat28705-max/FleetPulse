"""Edge Gateway Runtime: gom cua so 10s theo thoi gian, tinh dac trung qua fp_algo, CUSUM cuc bo, Store-and-Forward ben bi, phat heartbeat."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import sys
import time
from collections import defaultdict
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

from fp_algo.detect import Cusum
from fp_algo.features import window_features
from metrics import metrics

# Tai cau hinh .env
load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST") or "localhost"
MQTT_PORT = int(os.getenv("MQTT_HOST_PORT") or os.getenv("MQTT_PORT") or 1883)
WINDOW_SEC = int(os.getenv("WINDOW_SEC") or "10")
MAX_QUEUE_RECORDS = int(os.getenv("GATEWAY_MAX_QUEUE_RECORDS") or "50000")
LATENCY_THRESHOLD_SEC = float(os.getenv("GATEWAY_LATE_THRESHOLD_SEC") or "5.0")


def is_valid_float(val: Any) -> bool:
    """Kiem tra gia tri so co hop le va huu han hay khong (loai bo NaN, Inf)."""
    try:
        f = float(val)
        return not (math.isnan(f) or math.isinf(f))
    except (ValueError, TypeError):
        return False


def parse_iso_timestamp(ts_str: Any) -> float | None:
    """Parse chuoi ISO timestamp sang epoch seconds UTC."""
    if not isinstance(ts_str, str):
        return None
    try:
        clean_ts = ts_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean_ts)
        return dt.timestamp()
    except Exception:
        return None


class EdgeGateway:
    """Gateway xu ly tai bien cho mot nhom thiet bi."""

    def __init__(
        self,
        gateway_id: str = "GW-01",
        host: str = MQTT_HOST,
        port: int = MQTT_PORT,
        window_sec: int = WINDOW_SEC,
        max_queue_records: int = MAX_QUEUE_RECORDS,
        late_threshold_sec: float = LATENCY_THRESHOLD_SEC,
    ) -> None:
        self.gateway_id = gateway_id
        self.host = host
        self.port = port
        self.window_sec = max(1, int(window_sec))
        self.max_queue_records = max(100, int(max_queue_records))
        self.late_threshold_sec = max(0.5, float(late_threshold_sec))

        # Buffer cua so theo tung thiet bi: deviceId -> list[sample]
        self.buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.window_starts: dict[str, float] = {}
        self.device_meta: dict[str, dict[str, str]] = {}
        self.last_seen: dict[str, float] = {}

        # Bo CUSUM cuc bo cho tung thiet bi
        self.cusums: dict[str, Cusum] = defaultdict(lambda: Cusum(mu=0.42, sigma=0.15, k=0.5, h=3.5))

        # SQLite hang doi ben cho store-and-forward (P0: Alert, P1: Features, P2: Raw)
        db_dir = Path("data") / "gateway"
        db_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = db_dir / f"{self.gateway_id}_queue.db"
        self._init_sqlite()

        # Metrics van hanh thoi gian thuc
        self.start_time = time.time()
        self.messages_received = 0
        self.messages_invalid = 0
        self.windows_processed = 0
        self.windows_failed = 0
        self.publish_success = 0
        self.publish_failures = 0
        self.last_successful_sync: float | None = None

        # MQTT Client
        self.client = mqtt.Client(
            client_id=f"gateway_{self.gateway_id}",
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.is_connected = False
        self.last_heartbeat = 0.0

    def _init_sqlite(self) -> None:
        """Tao bang hang doi uu tien trong SQLite."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS priority_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    priority INTEGER, -- 0: P0 (Alert), 1: P1 (Features), 2: P2 (Raw)
                    topic TEXT,
                    payload TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_prio ON priority_queue(priority, id)")

    def _enqueue(self, priority: int, topic: str, payload_str: str) -> None:
        """Ghi vao hang doi SQLite voi co che bao ve dung luong va uu tien P0."""
        with sqlite3.connect(self.db_path) as conn:
            # Kiem tra dung luong hang doi
            row = conn.execute("SELECT COUNT(*) FROM priority_queue").fetchone()
            current_count = row[0] if row else 0

            # Neu vuot nguong toi da, loai bo bot cac ban ghi do uu tien thap nhat (P2 -> P1), tuyet doi giu P0
            if current_count >= self.max_queue_records:
                # Xoa bot 100 ban ghi cu nhat cua P2
                deleted = conn.execute(
                    "DELETE FROM priority_queue WHERE id IN (SELECT id FROM priority_queue WHERE priority = 2 ORDER BY id ASC LIMIT 100)"
                ).rowcount
                if deleted == 0:
                    # Neu het P2 thi xoa bot P1
                    conn.execute(
                        "DELETE FROM priority_queue WHERE id IN (SELECT id FROM priority_queue WHERE priority = 1 ORDER BY id ASC LIMIT 50)"
                    )
                print(f"[WARN] Hang doi Gateway {self.gateway_id} vuot nguong ({current_count} ban ghi), da giai phong bot ban ghi uu tien thap.")

            conn.execute(
                "INSERT INTO priority_queue (priority, topic, payload) VALUES (?, ?, ?)",
                (priority, topic, payload_str),
            )

    def _get_queue_depth(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT COUNT(*) FROM priority_queue").fetchone()
            return row[0] if row else 0

    def _flush_queue(self) -> None:
        """Xa hang doi khi co ket noi: uu tien P0 truoc, chi xoa ban ghi khi publish thanh cong."""
        if not self.is_connected:
            return

        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT id, priority, topic, payload FROM priority_queue ORDER BY priority ASC, id ASC LIMIT 50"
            ).fetchall()
            if not rows:
                return

            ids_to_delete = []
            for r_id, prio, topic, payload in rows:
                try:
                    # P0 Alert gui voi QoS 1 de dam bao tin cay, cac ban khac gui QoS 0
                    publish_qos = 1 if prio == 0 else 0
                    info = self.client.publish(topic, payload, qos=publish_qos)

                    # CHI xoa khoi SQLite khi publish() thuc su enqueue/gui thanh cong
                    if info.rc == mqtt.MQTT_ERR_SUCCESS:
                        ids_to_delete.append(r_id)
                        self.publish_success += 1
                        self.last_successful_sync = time.time()
                    else:
                        self.publish_failures += 1
                        # Gap loi gui thi dung flush de giu thu tu va khong xoa nham ban ghi sau
                        break
                except Exception as exc:
                    self.publish_failures += 1
                    print(f"[WARN] Exception khi flush queue id={r_id}: {exc}")
                    break

            if ids_to_delete:
                conn.executemany("DELETE FROM priority_queue WHERE id = ?", [(i,) for i in ids_to_delete])

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"[OK] Gateway {self.gateway_id} da ket noi MQTT {self.host}:{self.port}")
            self.is_connected = True
            client.subscribe("fleet/+/+/telemetry")
        else:
            print(f"[!] Gateway ket noi MQTT that bai: {rc}")

    def _on_disconnect(self, client, userdata, disconnect_flags, rc=None, properties=None):
        self.is_connected = False
        print(f"[!] Gateway {self.gateway_id} mat ket noi MQTT broker.")

    def _validate_payload(self, payload: Any) -> tuple[bool, str, dict[str, Any] | None]:
        """Xac thuc va lam sach payload telemetry truoc khi dua vao buffer."""
        if not isinstance(payload, dict):
            return False, "Payload phai la JSON object", None

        d_id = payload.get("deviceId")
        if not d_id or not isinstance(d_id, str) or not d_id.strip():
            return False, "Thieu hoac sai dinh dang deviceId", None
        d_id = d_id.strip()

        vals = payload.get("values")
        if not isinstance(vals, dict):
            return False, "Truong values phai la JSON object", None

        # Kiem tra cac truong so cot loi, loc bo NaN va Infinity
        for k in ["TP2", "TP3", "Motor_current", "Oil_temperature", "COMP"]:
            if k in vals and not is_valid_float(vals[k]):
                return False, f"Gia tri cam bien {k} khong hop le (NaN/Inf): {vals[k]}", None

        # Lay an toan cac truong so
        sample = {
            "TP2": float(vals.get("TP2", 0.0) if is_valid_float(vals.get("TP2")) else 0.0),
            "TP3": float(vals.get("TP3", 8.9) if is_valid_float(vals.get("TP3")) else 8.9),
            "Motor_current": float(vals.get("Motor_current", 0.0) if is_valid_float(vals.get("Motor_current")) else 0.0),
            "Oil_temperature": float(vals.get("Oil_temperature", 60.0) if is_valid_float(vals.get("Oil_temperature")) else 60.0),
            "COMP": float(vals.get("COMP", 1.0) if is_valid_float(vals.get("COMP")) else 1.0),
            "ts": payload.get("ts"),
            "emit_ts": payload.get("emit_ts"),
        }
        return True, d_id, sample

    def _on_message(self, client, userdata, msg):
        self.messages_received += 1
        now = time.time()
        try:
            raw_payload = json.loads(msg.payload.decode("utf-8"))
            valid, d_id_or_err, sample = self._validate_payload(raw_payload)
            if not valid:
                self.messages_invalid += 1
                return

            d_id = d_id_or_err
            line = str(raw_payload.get("line") or "L1")
            gw = str(raw_payload.get("gatewayId") or self.gateway_id)

            # Loc theo gatewayId neu khong phai 'all'
            if self.gateway_id != "all" and gw != self.gateway_id:
                return

            self.device_meta[d_id] = {"line": line, "gatewayId": gw}
            self.last_seen[d_id] = now

            if d_id not in self.window_starts:
                self.window_starts[d_id] = now

            self.buffers[d_id].append(sample)

            # CHỈ DÓNG CỬA SỔ THEO THỜI GIAN THỰC ĐÃ TRÔI QUA (time-based windowing)
            # Không dùng số lượng sample làm điều kiện đóng cửa sổ để tránh co cụm cửa sổ
            elapsed = now - self.window_starts[d_id]
            if elapsed >= self.window_sec:
                self._process_window(d_id, is_idle_flush=False)

        except Exception as e:
            self.messages_invalid += 1
            print(f"[!] Loi xu ly message tai gateway: {e}")

    def _process_window(self, device_id: str, is_idle_flush: bool = False) -> None:
        """Tinh dac trung cua so va phat len MQTT/enqueue vao SQLite."""
        samples = self.buffers.get(device_id, [])
        if not samples:
            return

        now_wall = time.time()
        meta = self.device_meta.get(device_id, {"line": "L1", "gatewayId": self.gateway_id})

        try:
            # 1. Tinh dac trung cua so truoc khi xoa buffer
            feat = window_features(samples)

            # 2. CUSUM phat hien bat thuong cuc bo
            cusum = self.cusums[device_id]
            alert = cusum.update(feat["loaded_ratio"])
            mode = "burst" if alert else "normal"
            metrics.gauge(
                "device.anomaly_score",
                float(cusum.pos),
                tags=[f"device:{device_id}", f"line:{meta['line']}"],
            )

            # 3. Kiem tra tinh chat late arrival
            first_sample = samples[0]
            w_start = first_sample.get("ts") or datetime.now(timezone.utc).isoformat()
            gw_now_dt = datetime.now(timezone.utc)
            gw_ts = gw_now_dt.isoformat()

            # Tinh do tre tu emit_ts den luc nhan tai gateway
            is_late = False
            latency_sec = 0.0
            emit_ts_str = first_sample.get("emit_ts")
            if emit_ts_str:
                emit_epoch = parse_iso_timestamp(emit_ts_str)
                if emit_epoch is not None:
                    latency_sec = max(0.0, now_wall - emit_epoch)
                    if latency_sec > self.late_threshold_sec:
                        is_late = True

            features_doc = {
                "deviceId": device_id,
                "line": meta["line"],
                "gatewayId": meta["gatewayId"],
                "windowStart": str(w_start),
                "windowSec": self.window_sec,
                "n": len(samples),
                "late": is_late,
                "incomplete": is_idle_flush,
                "feat": feat,
                "edge": {
                    "cusum_pos": round(cusum.pos, 3),
                    "alert": alert,
                    "mode": mode,
                },
                "ts_chain": {
                    "emit_ts": first_sample.get("emit_ts", gw_ts),
                    "gateway_ts": gw_ts,
                    "latency_sec": round(latency_sec, 3),
                },
            }

            topic = f"fleet/{meta['line']}/{device_id}/features"
            payload_str = json.dumps(features_doc)

            # 4. Phat tin hoac luu hang doi store-and-forward
            # P0: Canh bao nguy cap (Alert), P1: Dac trung thuong
            priority = 0 if alert else 1
            published_ok = False

            if self.is_connected:
                try:
                    publish_qos = 1 if priority == 0 else 0
                    info = self.client.publish(topic, payload_str, qos=publish_qos)
                    # Kiem tra ket qua tra ve ngay tai cho
                    if info.rc == mqtt.MQTT_ERR_SUCCESS:
                        published_ok = True
                        self.publish_success += 1
                        self.last_successful_sync = now_wall
                    else:
                        self.publish_failures += 1
                except Exception as exc:
                    self.publish_failures += 1
                    published_ok = False

            if not published_ok:
                # Luu vao SQLite neu chua gui thanh cong
                self._enqueue(priority, topic, payload_str)

            # 5. CHI SAU KHI XU LY VA PHAT/ENQUEUE THANH CONG MOI RESET BUFFER
            self.buffers[device_id] = []
            self.window_starts[device_id] = now_wall
            self.windows_processed += 1

        except Exception as exc:
            self.windows_failed += 1
            print(f"[!] Loi xu ly window_features cho thiet bi {device_id}: {exc}")
            # Khong xoa buffer de co the kiem tra lai hoac khong lam mat du lieu thieu

    def check_idle_windows(self) -> None:
        """Dong cac cua so bi 'treo' khi thiet bi tam ngung phat du lieu."""
        now = time.time()
        for d_id, start_ts in list(self.window_starts.items()):
            if d_id in self.buffers and self.buffers[d_id]:
                elapsed = now - start_ts
                # Neu da qua window_sec ma khong co message moi trong it nhat 2 giay -> flush idle window
                idle_time = now - self.last_seen.get(d_id, start_ts)
                if elapsed >= self.window_sec and idle_time >= 2.0:
                    self._process_window(d_id, is_idle_flush=True)

    def run(self) -> None:
        print(f"[*] Khoi dong Gateway {self.gateway_id}...")
        connected = False
        while not connected:
            try:
                self.client.connect(self.host, self.port, keepalive=60)
                connected = True
            except Exception as e:
                print(f"[!] Gateway {self.gateway_id} chua ket noi duoc MQTT ({e}). Thu lai sau 2 giay...")
                time.sleep(2.0)
        self.client.loop_start()

        try:
            while True:
                time.sleep(1.0)
                now = time.time()

                # 1. Kiem tra va flush cac cua so nhan roi
                self.check_idle_windows()

                # 2. Phat Heartbeat dinh ky va flush hang doi SQLite
                if now - self.last_heartbeat >= 10.0:
                    self.last_heartbeat = now
                    q_depth = self._get_queue_depth()
                    uptime = int(now - self.start_time)

                    hb = {
                        "gatewayId": self.gateway_id,
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "uptime_sec": uptime,
                        "queueDepth": q_depth,
                        "trackedDevices": list(self.device_meta.keys()),
                        "metrics": {
                            "messages_received": self.messages_received,
                            "messages_invalid": self.messages_invalid,
                            "windows_processed": self.windows_processed,
                            "windows_failed": self.windows_failed,
                            "publish_success": self.publish_success,
                            "publish_failures": self.publish_failures,
                            "is_connected": self.is_connected,
                            "last_successful_sync": self.last_successful_sync,
                        },
                    }
                    info = self.client.publish(f"fleet/heartbeat/{self.gateway_id}", json.dumps(hb), qos=0)
                    if info.rc == mqtt.MQTT_ERR_SUCCESS:
                        self.publish_success += 1
                    else:
                        self.publish_failures += 1

                    self._flush_queue()

        except KeyboardInterrupt:
            print(f"\n[*] Dang dung Gateway {self.gateway_id}...")
        finally:
            self.client.loop_stop()
            self.client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description="FleetPulse Edge Gateway")
    parser.add_argument("--gateway-id", default="all", help="Ma gateway (GW-01 hoac 'all')")
    parser.add_argument("--host", default=MQTT_HOST, help="MQTT host")
    parser.add_argument("--port", type=int, default=MQTT_PORT, help="MQTT port")
    parser.add_argument("--window-sec", type=int, default=WINDOW_SEC, help="Cua so tinh dac trung (giay)")
    args = parser.parse_args()

    gw = EdgeGateway(
        gateway_id=args.gateway_id,
        host=args.host,
        port=args.port,
        window_sec=args.window_sec,
    )
    gw.run()


if __name__ == "__main__":
    main()
