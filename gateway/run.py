"""Edge Gateway Runtime: gom cửa sổ 10s, tính đặc trưng qua fp_algo, CUSUM cục bộ, phát heartbeat."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
import paho.mqtt.client as mqtt

from fp_algo.detect import Cusum
from fp_algo.features import window_features
from metrics import metrics

# Tải cấu hình .env
load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST") or "localhost"
MQTT_PORT = int(os.getenv("MQTT_HOST_PORT") or os.getenv("MQTT_PORT") or 1884)
WINDOW_SEC = int(os.getenv("WINDOW_SEC") or "10")


class EdgeGateway:
    """Gateway xử lý tại biên cho một nhóm thiết bị."""

    def __init__(
        self,
        gateway_id: str = "GW-01",
        host: str = MQTT_HOST,
        port: int = MQTT_PORT,
        window_sec: int = WINDOW_SEC,
    ) -> None:
        self.gateway_id = gateway_id
        self.host = host
        self.port = port
        self.window_sec = window_sec

        # Buffer cửa sổ theo từng thiết bị: deviceId -> list[sample]
        self.buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.window_starts: dict[str, float] = {}
        self.device_meta: dict[str, dict[str, str]] = {}

        # Bộ CUSUM cục bộ cho từng thiết bị
        self.cusums: dict[str, Cusum] = defaultdict(lambda: Cusum(mu=0.42, sigma=0.15, k=0.5, h=3.5))

        # SQLite hàng đợi bền cho store-and-forward (P0: Alert, P1: Features, P2: Raw)
        db_dir = Path("data") / "gateway"
        db_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = db_dir / f"{self.gateway_id}_queue.db"
        self._init_sqlite()

        # MQTT Client
        self.client = mqtt.Client(client_id=f"gateway_{self.gateway_id}", callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.is_connected = False
        self.last_heartbeat = 0.0

    def _init_sqlite(self) -> None:
        """Tạo bảng hàng đợi ưu tiên trong SQLite."""
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
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO priority_queue (priority, topic, payload) VALUES (?, ?, ?)",
                (priority, topic, payload_str),
            )

    def _get_queue_depth(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT COUNT(*) FROM priority_queue").fetchone()
            return row[0] if row else 0

    def _flush_queue(self) -> None:
        """Xả hàng đợi khi có kết nối, ưu tiên P0 trước, rồi P1, rồi P2."""
        if not self.is_connected:
            return

        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT id, priority, topic, payload FROM priority_queue ORDER BY priority ASC, id ASC LIMIT 50").fetchall()
            if not rows:
                return

            ids_to_delete = []
            for r_id, prio, topic, payload in rows:
                try:
                    self.client.publish(topic, payload, qos=0)
                    ids_to_delete.append(r_id)
                except Exception:
                    break

            if ids_to_delete:
                conn.executemany("DELETE FROM priority_queue WHERE id = ?", [(i,) for i in ids_to_delete])

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"[OK] Gateway {self.gateway_id} đã kết nối MQTT {self.host}:{self.port}")
            self.is_connected = True
            # Lắng nghe telemetry của toàn bộ hệ thống hoặc nhóm của mình
            client.subscribe("fleet/+/+/telemetry")
        else:
            print(f"[!] Gateway kết nối MQTT thất bại: {rc}")

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            d_id = payload.get("deviceId")
            line = payload.get("line", "L1")
            gw = payload.get("gatewayId", self.gateway_id)

            # Nếu bản tin thuộc gateway này phụ trách (hoặc chế độ đón tất cả nếu gateway_id = 'all')
            if self.gateway_id != "all" and gw != self.gateway_id:
                return

            self.device_meta[d_id] = {"line": line, "gatewayId": gw}

            vals = payload.get("values", {})
            sample = {
                "TP2": vals.get("TP2", 0.0),
                "TP3": vals.get("TP3", 8.9),
                "Motor_current": vals.get("Motor_current", 0.0),
                "Oil_temperature": vals.get("Oil_temperature", 60.0),
                "COMP": vals.get("COMP", 1),
                "ts": payload.get("ts"),
                "emit_ts": payload.get("emit_ts"),
            }

            now = time.time()
            if d_id not in self.window_starts:
                self.window_starts[d_id] = now

            self.buffers[d_id].append(sample)

            # Kiểm tra xem đã đầy cửa sổ chưa (theo thời gian hoặc đủ số mẫu)
            elapsed = now - self.window_starts[d_id]
            if elapsed >= self.window_sec or len(self.buffers[d_id]) >= max(1, self.window_sec):
                self._process_window(d_id)

        except Exception as e:
            print(f"[!] Lỗi xử lý message tại gateway: {e}")

    def _process_window(self, device_id: str) -> None:
        samples = self.buffers[device_id]
        if not samples:
            return

        self.buffers[device_id] = []
        self.window_starts[device_id] = time.time()

        meta = self.device_meta.get(device_id, {"line": "L1", "gatewayId": self.gateway_id})
        feat = window_features(samples)

        # CUSUM cục bộ kiểm tra loaded_ratio hoặc TP2
        cusum = self.cusums[device_id]
        alert = cusum.update(feat["loaded_ratio"])
        mode = "burst" if alert else "normal"
        metrics.gauge("device.anomaly_score", float(cusum.pos), tags=[f"device:{device_id}", f"line:{meta['line']}"])

        first_sample = samples[0]
        w_start = first_sample.get("ts") or datetime.now(timezone.utc).isoformat()
        gw_ts = datetime.now(timezone.utc).isoformat()

        features_doc = {
            "deviceId": device_id,
            "line": meta["line"],
            "gatewayId": meta["gatewayId"],
            "windowStart": str(w_start),
            "windowSec": self.window_sec,
            "n": len(samples),
            "late": False,
            "feat": feat,
            "edge": {
                "cusum_pos": round(cusum.pos, 3),
                "alert": alert,
                "mode": mode,
            },
            "ts_chain": {
                "emit_ts": first_sample.get("emit_ts", gw_ts),
                "gateway_ts": gw_ts,
            },
        }

        topic = f"fleet/{meta['line']}/{device_id}/features"
        payload_str = json.dumps(features_doc)

        priority = 0 if alert else 1
        if self.is_connected:
            try:
                self.client.publish(topic, payload_str, qos=0)
            except Exception:
                self._enqueue(priority, topic, payload_str)
        else:
            self._enqueue(priority, topic, payload_str)

    def run(self) -> None:
        print(f"[*] Khởi động Gateway {self.gateway_id}...")
        self.client.connect(self.host, self.port, keepalive=60)
        self.client.loop_start()

        try:
            while True:
                time.sleep(1.0)
                # Gửi heartbeat định kỳ 10 giây
                now = time.time()
                if now - self.last_heartbeat >= 10.0:
                    self.last_heartbeat = now
                    q_depth = self._get_queue_depth()
                    hb = {
                        "gatewayId": self.gateway_id,
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "queueDepth": q_depth,
                        "trackedDevices": list(self.device_meta.keys()),
                    }
                    self.client.publish(f"fleet/heartbeat/{self.gateway_id}", json.dumps(hb), qos=0)
                    self._flush_queue()

        except KeyboardInterrupt:
            print(f"\n[*] Đang dừng Gateway {self.gateway_id}...")
        finally:
            self.client.loop_stop()
            self.client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description="FleetPulse Edge Gateway")
    parser.add_argument("--gateway-id", default="all", help="Mã gateway (GW-01, GW-02,... hoặc 'all' để nhận tất cả)")
    parser.add_argument("--host", default=MQTT_HOST, help="MQTT host")
    parser.add_argument("--port", type=int, default=MQTT_PORT, help="MQTT port")
    parser.add_argument("--window-sec", type=int, default=WINDOW_SEC, help="Kích thước cửa sổ tính đặc trưng (giây)")
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
