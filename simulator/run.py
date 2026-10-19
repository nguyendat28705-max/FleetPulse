"""Simulator phát lại dữ liệu MetroPT-3 cho N thiết bị ảo, hỗ trợ tiêm lỗi và phát MQTT."""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
import numpy as np
import paho.mqtt.client as mqtt
import pandas as pd
from pymongo import MongoClient

# Tải cấu hình .env
load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
PARQUET_PATH = ROOT / "data" / "processed" / "metropt3.parquet"

MONGO_URI = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI") or "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
DB_NAME = os.getenv("MONGODB_DATABASE") or os.getenv("MONGO_DB") or "fleetpulse_dev"
MQTT_HOST = os.getenv("MQTT_HOST") or "localhost"
MQTT_PORT = int(os.getenv("MQTT_HOST_PORT") or os.getenv("MQTT_PORT") or 1884)


class VirtualDevice:
    """Đại diện cho 1 thiết bị ảo sinh ra từ dữ liệu MetroPT-3."""

    def __init__(
        self,
        device_id: str,
        line: str,
        gateway_id: str,
        start_idx: int,
        noise_std: float,
        bias_tp2: float,
        bias_temp: float,
    ) -> None:
        self.device_id = device_id
        self.line = line
        self.gateway_id = gateway_id
        self.curr_idx = start_idx
        self.noise_std = noise_std
        self.bias_tp2 = bias_tp2
        self.bias_temp = bias_temp
        self.seq = 1

        # Trạng thái tiêm lỗi
        self.active_fault: dict[str, Any] | None = None
        self.fault_start_time: float | None = None


class FleetSimulator:
    """Bộ mô phỏng đội tàu phát luồng MQTT."""

    def __init__(
        self,
        num_devices: int = 20,
        speed_factor: float = 60.0,
        seed: int = 42,
        host: str = MQTT_HOST,
        port: int = MQTT_PORT,
    ) -> None:
        self.num_devices = num_devices
        self.speed_factor = max(1.0, float(speed_factor))
        self.seed = seed
        self.host = host
        self.port = port

        random.seed(seed)
        np.random.seed(seed)

        print(f"[*] Đang tải dữ liệu gốc từ {PARQUET_PATH.relative_to(ROOT)}...")
        self.df = pd.read_parquet(PARQUET_PATH)
        self.total_rows = len(self.df)
        print(f"[OK] Đã tải {self.total_rows:,} dòng dữ liệu.")

        # Khởi tạo đội thiết bị ảo
        self.devices: list[VirtualDevice] = []
        for i in range(num_devices):
            d_id = f"M-{i+1:03d}"
            line = f"L{(i // 10) + 1}"
            gw = f"GW-{(i // 5) + 1:02d}"
            # Dịch thời gian mỗi máy một khoảng khác nhau (tối đa 100,000 dòng ~ 11 ngày)
            start_offset = random.randint(0, min(100_000, self.total_rows - 50_000))
            noise_std = random.uniform(0.01, 0.04)
            bias_tp2 = random.gauss(0.0, 0.05)
            bias_temp = random.gauss(0.0, 1.2)

            self.devices.append(
                VirtualDevice(
                    device_id=d_id,
                    line=line,
                    gateway_id=gw,
                    start_idx=start_offset,
                    noise_std=noise_std,
                    bias_tp2=bias_tp2,
                    bias_temp=bias_temp,
                )
            )

        # Kết nối MongoDB để theo dõi fault_labels
        self.mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        self.db = self.mongo_client[DB_NAME]

        # Khởi tạo MQTT client
        self.client = mqtt.Client(client_id="fleetpulse_simulator", callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.is_connected = False
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"[OK] Simulator đã kết nối MQTT broker tại {self.host}:{self.port}")
            self.is_connected = True
        else:
            print(f"[!] Lỗi kết nối MQTT, mã: {rc}")

    def _on_disconnect(self, client, userdata, rc, properties=None):
        self.is_connected = False

    def sync_faults(self) -> None:
        """Đọc các nhãn lỗi từ MongoDB."""
        try:
            faults = list(self.db["fault_labels"].find({"resolved": {"$ne": True}}))
            fault_map = {f["deviceId"]: f for f in faults}
            now_ts = time.time()
            for dev in self.devices:
                if dev.device_id in fault_map:
                    if dev.active_fault is None:
                        dev.active_fault = fault_map[dev.device_id]
                        dev.fault_start_time = now_ts
                        print(f"[!] Thiết bị {dev.device_id} bắt đầu tiêm lỗi: {dev.active_fault['type']} (mức={dev.active_fault['severity']})")
                else:
                    if dev.active_fault is not None:
                        print(f"[*] Thiết bị {dev.device_id} kết thúc lỗi.")
                        dev.active_fault = None
                        dev.fault_start_time = None
        except Exception as e:
            # Không để ngắt vòng lặp simulator nếu mongo tạm thời bận
            pass

    def run(self) -> None:
        """Chạy vòng lặp phát bản tin."""
        print(f"[*] Kết nối tới MQTT {self.host}:{self.port}...")
        self.client.connect(self.host, self.port, keepalive=60)
        self.client.loop_start()

        time.sleep(1.0)
        print(f"[*] Bắt đầu phát dữ liệu cho {self.num_devices} thiết bị (tốc độ={self.speed_factor}x)...")

        last_fault_check = 0.0

        try:
            while True:
                now_wall = time.time()
                if now_wall - last_fault_check > 5.0:
                    self.sync_faults()
                    last_fault_check = now_wall

                emit_iso = datetime.now(timezone.utc).isoformat()

                for dev in self.devices:
                    row = self.df.iloc[dev.curr_idx]
                    dev.curr_idx = (dev.curr_idx + 1) % self.total_rows

                    tp2 = float(row.get("TP2", 0.0)) + dev.bias_tp2 + random.gauss(0.0, dev.noise_std)
                    tp3 = float(row.get("TP3", 8.9)) + dev.bias_tp2
                    comp = float(row.get("COMP", 1.0))
                    motor = float(row.get("Motor_current", 0.0))
                    temp = float(row.get("Oil_temperature", 60.0)) + dev.bias_temp

                    # Tiêm lỗi rò khí nếu có
                    if dev.active_fault and dev.active_fault.get("type") == "air_leak":
                        sev = float(dev.active_fault.get("severity", 0.6))
                        # Tăng thời gian nạp khí (COMP = 0) và làm giảm áp suất
                        if random.random() < (0.4 + 0.5 * sev):
                            comp = 0.0  # buộc máy chạy tải
                            motor = max(motor, 6.8 + random.uniform(0.0, 1.5))
                            temp += 8.0 * sev
                        tp2 -= 0.8 * sev

                    payload = {
                        "deviceId": dev.device_id,
                        "line": dev.line,
                        "gatewayId": dev.gateway_id,
                        "seq": dev.seq,
                        "ts": str(row["timestamp"]),
                        "emit_ts": emit_iso,
                        "values": {
                            "TP2": round(tp2, 3),
                            "TP3": round(tp3, 3),
                            "H1": round(float(row.get("H1", 7.5)), 3),
                            "DV_pressure": round(float(row.get("DV_pressure", 0.0)), 3),
                            "Reservoirs": round(float(row.get("Reservoirs", 8.9)), 3),
                            "Motor_current": round(motor, 3),
                            "Oil_temperature": round(temp, 2),
                            "COMP": int(round(comp)),
                            "DV_eletric": int(round(float(row.get("DV_eletric", 0)))),
                            "Towers": int(round(float(row.get("Towers", 1)))),
                            "MPG": int(round(float(row.get("MPG", 1)))),
                            "LPS": int(round(float(row.get("LPS", 0)))),
                            "Pressure_switch": int(round(float(row.get("Pressure_switch", 1)))),
                            "Oil_level": int(round(float(row.get("Oil_level", 1)))),
                            "Caudal_impulses": int(round(float(row.get("Caudal_impulses", 1)))),
                        },
                    }

                    topic = f"fleet/{dev.line}/{dev.device_id}/telemetry"
                    self.client.publish(topic, json.dumps(payload), qos=0)
                    dev.seq += 1

                # Khoảng nghỉ theo hệ số tăng tốc (dữ liệu cách 10s / speed_factor)
                step_sleep = max(0.05, 10.0 / self.speed_factor)
                time.sleep(step_sleep)

        except KeyboardInterrupt:
            print("\n[*] Dừng simulator theo yêu cầu.")
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            self.mongo_client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="FleetPulse Simulator")
    parser.add_argument("--devices", type=int, default=int(os.getenv("FLEET_SIZE", "20")), help="Số lượng thiết bị")
    parser.add_argument("--speed", type=float, default=float(os.getenv("SPEED_FACTOR", "60")), help="Hệ số tăng tốc thời gian")
    parser.add_argument("--seed", type=int, default=int(os.getenv("RANDOM_SEED", "42")), help="Hạt giống sinh ngẫu nhiên")
    parser.add_argument("--host", default=MQTT_HOST, help="Địa chỉ MQTT Broker")
    parser.add_argument("--port", type=int, default=MQTT_PORT, help="Cổng MQTT Broker")
    args = parser.parse_args()

    sim = FleetSimulator(
        num_devices=args.devices,
        speed_factor=args.speed,
        seed=args.seed,
        host=args.host,
        port=args.port,
    )
    sim.run()


if __name__ == "__main__":
    main()
