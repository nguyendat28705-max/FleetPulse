"""Unified FleetPulse Runner: Chạy đồng thời toàn bộ 5 dịch vụ lõi trên các luồng nền.

Bao gồm:
1. Ingestion Service (MQTT -> MongoDB Time Series & features_live)
2. Edge Gateway (Gom cửa sổ 10s, CUSUM)
3. Streaming Engine (MongoDB Change Streams, AI/Baseline Inference, Incidents)
4. Fleet Simulator (20 thiết bị M-001 -> M-020, tốc độ 60x)
5. Scheduler FastAPI Service (Cổng 8002)
"""

from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path

# Đảm bảo UTF-8 an toàn trên Windows console
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
import uvicorn

from engine.run import StreamingEngine
from gateway.run import EdgeGateway
from ingestion.run import IngestionService
from mongo.client import get_mongo_database_name, get_mongo_uri, sanitize_uri
from scheduler.app import app as fastapi_app
from simulator.run import FleetSimulator

load_dotenv()

MONGO_URI = get_mongo_uri()
DB_NAME = get_mongo_database_name()
MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_HOST_PORT", "1883"))
PREDICTOR_MODE = os.getenv("PREDICTOR_MODE", "ai")
AI_MODEL_PATH = os.getenv("AI_MODEL_PATH", "models/fleetpulse_model.joblib")
FLEET_SIZE = int(os.getenv("FLEET_SIZE", "20"))
SPEED_FACTOR = float(os.getenv("SPEED_FACTOR", "60.0"))


def run_scheduler_uvicorn():
    uvicorn.run(fastapi_app, host="0.0.0.0", port=8002, log_level="warning")


def main():
    print("=" * 68)
    print("       FLEETPULSE UNIFIED RUNNER - END-TO-END SYSTEM ONLINE         ")
    print("=" * 68)
    print(f"[*] MongoDB Target : {sanitize_uri(MONGO_URI)} -> DB: {DB_NAME}")
    print(f"[*] MQTT Broker    : {MQTT_HOST}:{MQTT_PORT}")
    print(f"[*] Predictor Mode : {PREDICTOR_MODE.upper()} ({AI_MODEL_PATH})")
    print(f"[*] Fleet Size     : {FLEET_SIZE} trains (Speed {SPEED_FACTOR}x)")
    print(f"[*] Scheduler API  : http://localhost:8002/work_orders")
    print("-" * 68)

    # 1. Khởi tạo Ingestion
    ingest = IngestionService(
        mongo_uri=MONGO_URI,
        db_name=DB_NAME,
        host=MQTT_HOST,
        port=MQTT_PORT,
    )

    # 2. Khởi tạo Gateway
    gw = EdgeGateway(
        gateway_id="all",
        host=MQTT_HOST,
        port=MQTT_PORT,
        window_sec=10,
    )

    # 3. Khởi tạo Streaming Engine
    engine = StreamingEngine(
        shard_id="all",
        mongo_uri=MONGO_URI,
        db_name=DB_NAME,
        predictor_mode=PREDICTOR_MODE,
        model_path=AI_MODEL_PATH,
    )

    # 4. Khởi tạo Simulator
    sim = FleetSimulator(
        num_devices=FLEET_SIZE,
        speed_factor=SPEED_FACTOR,
        seed=42,
        host=MQTT_HOST,
        port=MQTT_PORT,
        mongo_uri=MONGO_URI,
        db_name=DB_NAME,
    )

    # 5. Chạy các dịch vụ trên các background threads
    threads = [
        threading.Thread(target=ingest.run, daemon=True, name="IngestionThread"),
        threading.Thread(target=gw.run, daemon=True, name="GatewayThread"),
        threading.Thread(target=engine.run, daemon=True, name="EngineThread"),
        threading.Thread(target=run_scheduler_uvicorn, daemon=True, name="SchedulerThread"),
        threading.Thread(target=sim.run, daemon=True, name="SimulatorThread"),
    ]

    for t in threads:
        t.start()
        time.sleep(0.5)

    print("\n[OK] Toan bo 5 thanh phan cua he thong dang hoat dong dong thoi!")
    print("Nhan Ctrl+C de dung toan bo.\n")

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\n[*] Dang dung he thong FleetPulse...")
        sys.exit(0)


if __name__ == "__main__":
    main()

