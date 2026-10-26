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

# Đảm bảo UTF-8 an toàn trên Windows console
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
import numpy as np
import paho.mqtt.client as mqtt
import pandas as pd

from mongo.client import (
    create_mongo_client,
    get_mongo_database_name,
    get_mongo_uri,
    sanitize_uri,
)

# Tải cấu hình .env
load_dotenv()

PARQUET_PATH = ROOT / "data" / "processed" / "metropt3.parquet"

MONGO_URI = get_mongo_uri()
DB_NAME = get_mongo_database_name()
MQTT_HOST = os.getenv("MQTT_HOST") or "localhost"
MQTT_PORT = int(os.getenv("MQTT_HOST_PORT") or os.getenv("MQTT_PORT") or 1883)

REQUIRED_PARQUET_COLUMNS = {
    "timestamp",
    "TP2",
    "TP3",
    "COMP",
    "Motor_current",
    "Oil_temperature",
}


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
        self.trigger_ts: float | None = None


class FleetSimulator:
    """Bộ mô phỏng đội tàu phát luồng MQTT."""

    def __init__(
        self,
        num_devices: int = 20,
        speed_factor: float = 60.0,
        seed: int = 42,
        host: str = MQTT_HOST,
        port: int = MQTT_PORT,
        mongo_uri: str = MONGO_URI,
        db_name: str = DB_NAME,
    ) -> None:
        self.num_devices = num_devices
        self.speed_factor = max(1.0, float(speed_factor))
        self.seed = seed
        self.host = host
        self.port = port
        self.mongo_uri = mongo_uri
        self.db_name = db_name

        # Thong so giam sat van hanh
        self.messages_sent = 0
        self.messages_dropped = 0
        self.last_sync_time: float | None = None
        self._last_mongo_err_log = 0.0

        random.seed(seed)
        np.random.seed(seed)

        # 1. Kiem tra va nạp du lieu goc Parquet
        self._load_and_validate_dataset()

        # 2. Khoi tao doi thiet bi ao
        self.devices: list[VirtualDevice] = []
        for i in range(num_devices):
            d_id = f"M-{i+1:03d}"
            line = f"L{(i // 10) + 1}"
            gw = f"GW-{(i // 5) + 1:02d}"

            # Tinh toan start_offset an toan voi moi kich thuoc dataframe
            max_offset = max(0, min(100_000, self.total_rows - 50_000))
            if max_offset == 0 and self.total_rows > 1:
                max_offset = max(0, self.total_rows - 10)
            start_offset = random.randint(0, max_offset) if max_offset > 0 else 0

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

        # 3. Ket noi MongoDB de theo doi fault_labels
        self.mongo_client = create_mongo_client(self.mongo_uri)
        self.db = self.mongo_client[self.db_name]
        print(f"[*] Simulator ket noi MongoDB ({sanitize_uri(self.mongo_uri)}) database: {self.db_name}")

        # 4. Khoi tao MQTT client
        self.client = mqtt.Client(
            client_id="fleetpulse_simulator",
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        self.is_connected = False
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def _load_and_validate_dataset(self) -> None:
        """Kiem tra tap du lieu Parquet truoc khi khoi dong."""
        if not PARQUET_PATH.exists():
            print(f"[!] Loi: Khong tim thay tap du lieu goc tai {PARQUET_PATH}.", file=sys.stderr)
            print("    Vui long chay: python scripts/eda_datasets.py de tien xu ly du lieu MetroPT-3.", file=sys.stderr)
            sys.exit(1)

        print(f"[*] Dang tai du lieu goc tu {PARQUET_PATH.name}...")
        try:
            self.df = pd.read_parquet(PARQUET_PATH)
        except Exception as exc:
            print(f"[!] Loi khi doc file Parquet tai {PARQUET_PATH}: {exc}", file=sys.stderr)
            sys.exit(1)

        self.total_rows = len(self.df)
        if self.total_rows == 0:
            print(f"[!] Loi: File Parquet {PARQUET_PATH} khong co dong du lieu nao.", file=sys.stderr)
            sys.exit(1)

        missing_cols = REQUIRED_PARQUET_COLUMNS - set(self.df.columns)
        if missing_cols:
            print(
                f"[!] Loi: File Parquet thieu cac cot bat buoc: {sorted(missing_cols)}.",
                file=sys.stderr,
            )
            sys.exit(1)

        print(f"[OK] Da tai va xac thuc {self.total_rows:,} dong du lieu Parquet thanh cong.")

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"[OK] Simulator da ket noi MQTT broker tai {self.host}:{self.port}")
            self.is_connected = True
        else:
            print(f"[!] Loi ket noi MQTT, ma: {rc}")

    def _on_disconnect(self, client, userdata, flags_or_rc, rc=None, properties=None):
        self.is_connected = False
        print("[!] Simulator mat ket noi MQTT broker.")

    def sync_faults(self) -> None:
        """Doc cac nhan loi tu MongoDB voi quy tac uu tien ro rang va ho tro offset_sec."""
        now_wall = time.time()
        try:
            # Sap xep theo createdAt giam dan de dam bao loi moi nhat duoc uu tien
            faults = list(
                self.db["fault_labels"]
                .find({"resolved": {"$ne": True}})
                .sort("createdAt", -1)
            )

            # Map deviceId -> fault moi nhat
            fault_map: dict[str, dict[str, Any]] = {}
            duplicate_devs: set[str] = set()

            for f in faults:
                d_id = f.get("deviceId")
                if not d_id:
                    continue
                if d_id not in fault_map:
                    fault_map[d_id] = f
                else:
                    duplicate_devs.add(d_id)

            if duplicate_devs:
                print(f"[WARN] Phat hien nhieu loi active chua resolve cho thiet bi: {sorted(duplicate_devs)}. Uu tien loi moi nhat.")

            for dev in self.devices:
                if dev.device_id in fault_map:
                    fault = fault_map[dev.device_id]
                    fault_id = str(fault.get("_id"))
                    current_id = str(dev.active_fault.get("_id")) if dev.active_fault else None

                    # Neu la loi moi hoac thay doi id loi
                    if current_id != fault_id:
                        dev.active_fault = fault
                        dev.fault_start_time = now_wall

                        # Tinh toan thoi diem trigger thuc te dua tren t0 + offset_sec
                        t0 = fault.get("t0")
                        offset_sec = float(fault.get("offset_sec", 0) or 0)
                        if isinstance(t0, datetime):
                            base_ts = t0.timestamp()
                        else:
                            base_ts = now_wall

                        dev.trigger_ts = base_ts + offset_sec
                        delay = max(0.0, dev.trigger_ts - now_wall)

                        print(
                            f"[!] Thiet bi {dev.device_id} tiep nhan loi: {fault.get('type')} "
                            f"(severity={fault.get('severity')}, offset={offset_sec:.0f}s, kich hoat sau={delay:.1f}s)"
                        )
                else:
                    if dev.active_fault is not None:
                        print(f"[*] Thiet bi {dev.device_id} ket thuc loi.")
                        dev.active_fault = None
                        dev.fault_start_time = None
                        dev.trigger_ts = None

            self.last_sync_time = now_wall

        except Exception as exc:
            # Throttle log loi ket noi MongoDB (khong spam moi chu ky)
            if now_wall - self._last_mongo_err_log > 15.0:
                print(f"[WARN] Khong the dong bo fault_labels tu MongoDB: {exc}", file=sys.stderr)
                self._last_mongo_err_log = now_wall

    def run(self) -> None:
        """Chay vong lap phat ban tin."""
        print(f"[*] Ket noi toi MQTT {self.host}:{self.port}...")
        connected = False
        while not connected:
            try:
                self.client.connect(self.host, self.port, keepalive=60)
                connected = True
            except Exception as e:
                print(f"[!] Simulator chua ket noi duoc MQTT ({e}). Thu lai sau 2 giay...")
                time.sleep(2.0)
        self.client.loop_start()

        time.sleep(1.0)
        print(f"[*] Bat dau phat du lieu cho {self.num_devices} thiet bi (toc do={self.speed_factor}x)...")

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

                    # Kiem tra xem loi da den thoi diem kich hoat (offset_sec) hay chua
                    is_fault_active = False
                    if dev.active_fault and dev.trigger_ts is not None:
                        if now_wall >= dev.trigger_ts:
                            is_fault_active = True

                    # Ap dung bien doi du lieu theo tung loai loi
                    if is_fault_active and dev.active_fault:
                        fault_type = dev.active_fault.get("type")
                        sev = float(dev.active_fault.get("severity", 0.6))

                        if fault_type == "air_leak":
                            # 1. Ro ri khi: may nen phai chay nap thuong xuyen hon, tut ap TP2, tang dong motor va nhiet do
                            if random.random() < (0.4 + 0.5 * sev):
                                comp = 0.0  # buoc may chay tai nap khi
                                motor = max(motor, 6.8 + random.uniform(0.0, 1.5))
                                temp += 8.0 * sev
                            tp2 -= 0.8 * sev

                        elif fault_type == "sensor_drift":
                            # 2. Troi dat cam bien: tin hieu do bi lech gia tri gia tao so voi thuc te
                            # Do lech ty le voi severity lam sai lech dac trung delta_pressure va tp2_mean
                            tp2 += 1.5 * sev
                            temp += 12.0 * sev

                        elif fault_type == "stuck_comp":
                            # 3. Ket may nen / van nap: COMP lien tuc o che do nap tai (0.0), dong co chay tai nang lien tuc
                            comp = 0.0
                            motor = max(motor, 7.5 + 0.8 * sev + random.uniform(0.0, 0.8))
                            temp += 15.0 * sev
                            tp2 = max(tp2, 8.8 + random.uniform(0.0, 0.6))

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
                    info = self.client.publish(topic, json.dumps(payload), qos=0)

                    # Kiem tra ket qua publish MQTT
                    if info.rc == mqtt.MQTT_ERR_SUCCESS:
                        dev.seq += 1
                        self.messages_sent += 1
                    else:
                        self.messages_dropped += 1
                        if self.messages_dropped % 100 == 1:
                            print(f"[WARN] Publish MQTT that bai voi ma rc={info.rc}. Tong dropped: {self.messages_dropped}")

                # Khoảng nghỉ theo hệ số tăng tốc (dữ liệu cách 10s / speed_factor)
                step_sleep = max(0.05, 10.0 / self.speed_factor)
                time.sleep(step_sleep)

        except KeyboardInterrupt:
            print(f"\n[*] Dung simulator theo yeu cau. Da phat: {self.messages_sent:,} ban tin (dropped: {self.messages_dropped}).")
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
    parser.add_argument("--mongo-uri", default=MONGO_URI, help="MongoDB connection URI")
    parser.add_argument("--db", default=DB_NAME, help="Database name")
    args = parser.parse_args()

    sim = FleetSimulator(
        num_devices=args.devices,
        speed_factor=args.speed,
        seed=args.seed,
        host=args.host,
        port=args.port,
        mongo_uri=args.mongo_uri,
        db_name=args.db,
    )
    sim.run()


if __name__ == "__main__":
    main()
