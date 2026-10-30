"""End-to-End Live Integration Test: Kiem thu thuc te toan bo he thong tren Docker ha tang.

Quy trinh test:
1. Khoi tao database test tren Docker MongoDB (rs0) va MQTT (Mosquitto 1883).
2. Chay dong thoi: Ingestion, Gateway, Streaming Engine, Simulator.
3. Kiem tra luong du lieu binh thuong: Telemetry -> Features -> Device State.
4. Tiem loi ro khi (air_leak) cho M-002 va xac nhan Incident duoc tao tu dong.
5. Chay Scheduler de xac nhan Work Order duoc tao Idempotent (khong trung lap).
6. Xac nhan incident duoc dong (resolved) khi thiet bi phuc hoi ve OK.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pymongo import MongoClient

from engine.run import StreamingEngine
from gateway.run import EdgeGateway
from ingestion.run import IngestionService
from scheduler.app import run_greedy_scheduler
from simulator.inject import inject_fault
from simulator.run import FleetSimulator

TEST_MONGO_URI = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
TEST_DB_NAME = "fleetpulse_e2e_live_test"
MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883


def setup_test_collections(client: MongoClient) -> None:
    """Khoi tao cac collection va index can thiet tren database test."""
    db = client[TEST_DB_NAME]
    # Drop database test cu neu co de bat dau sach se
    client.drop_database(TEST_DB_NAME)

    # 1. Telemetry Timeseries
    db.create_collection(
        "telemetry",
        timeseries={"timeField": "ts", "metaField": "meta", "granularity": "seconds"},
    )
    db["telemetry"].create_index([("meta.deviceId", 1), ("ts", -1)])

    # 2. Features live
    db.create_collection("features_live")
    db["features_live"].create_index([("deviceId", 1), ("windowStart", 1)], unique=True)
    db["features_live"].create_index([("createdAt", 1)], expireAfterSeconds=3600)

    # 3. Device state
    db.create_collection("device_state")
    db["device_state"].create_index([("deviceId", 1)], unique=True)

    # 4. Incidents & Work orders
    db.create_collection("incidents")
    db["incidents"].create_index([("deviceId", 1), ("status", 1)])

    db.create_collection("work_orders")
    db["work_orders"].create_index([("deviceId", 1), ("status", 1)])

    # 5. Fault labels & Checkpoints
    db.create_collection("fault_labels")
    db.create_collection("engine_checkpoints")
    print(f"[OK] Da khoi tao moi toan bo collections tren database test: {TEST_DB_NAME}")


def run_live_test():
    print("=" * 70)
    print("       FLEETPULSE - END-TO-END LIVE INTEGRATION & STRESS TEST        ")
    print("=" * 70)

    client = MongoClient(TEST_MONGO_URI, serverSelectionTimeoutMS=4000)
    try:
        client.admin.command("ping")
        print("[OK] Ket noi toi Docker MongoDB (rs0) thanh cong.")
    except Exception as exc:
        print(f"[!] Khong the ket noi toi Docker MongoDB tai {TEST_MONGO_URI}: {exc}")
        sys.exit(1)

    setup_test_collections(client)
    db = client[TEST_DB_NAME]

    # Khoi tao cac service tren database test
    print("\n[*] Khoi dong cac service Ingestion, Gateway, Streaming Engine...")
    ingestion = IngestionService(
        mongo_uri=TEST_MONGO_URI,
        db_name=TEST_DB_NAME,
        host=MQTT_HOST,
        port=MQTT_PORT,
        batch_size=10,
        flush_interval=0.5,
    )
    gateway = EdgeGateway(
        gateway_id="all",
        host=MQTT_HOST,
        port=MQTT_PORT,
        window_sec=2,  # Dat cua so 2 giay de live test phan hoi nhanh
    )
    engine = StreamingEngine(
        shard_id="all",
        mongo_uri=TEST_MONGO_URI,
        db_name=TEST_DB_NAME,
        predictor_mode="baseline",
    )
    simulator = FleetSimulator(
        num_devices=3,
        speed_factor=120.0,  # Toc do 120x
        seed=42,
        host=MQTT_HOST,
        port=MQTT_PORT,
        mongo_uri=TEST_MONGO_URI,
        db_name=TEST_DB_NAME,
    )

    # Chay cac service trong cac background threads
    threads = []
    t_ingest = threading.Thread(target=ingestion.run, daemon=True)
    t_gw = threading.Thread(target=gateway.run, daemon=True)
    t_eng = threading.Thread(target=engine.run, daemon=True)
    t_sim = threading.Thread(target=simulator.run, daemon=True)

    threads.extend([t_ingest, t_gw, t_eng, t_sim])
    for t in threads:
        t.start()

    print("[OK] Toan bo 4 thanh phan dang chay tren luong song song.")

    try:
        # BƯỚC 1: Cho luồng dữ liệu bình thường chảy qua
        print("\n--- [GIAI DOAN 1: KIEM TRA LUONG DU LIEU BINH THUONG] ---")
        time.sleep(6.0)

        telemetry_count = db["telemetry"].count_documents({})
        features_count = db["features_live"].count_documents({})
        states_count = db["device_state"].count_documents({})

        print(f"[*] So ban ghi telemetry nhan duoc : {telemetry_count:>5}")
        print(f"[*] So ban ghi features_live tao ra : {features_count:>5}")
        print(f"[*] So thiet bi cap nhat trang thai: {states_count:>5}")

        assert telemetry_count > 0, "Loi: Telemetry chua duoc ghi vao MongoDB!"
        assert features_count > 0, "Loi: Gateway chua tinh duoc features_live!"
        assert states_count >= 3, "Loi: Engine chua cap nhat device_state cho ca 3 thiet bi!"
        print("[PASS] Giai doan 1: Luong du lieu co ban (Raw -> Gateway -> Ingestion -> Engine) chay hoan hao.")

        # BƯỚC 2: Tiêm lỗi rò khí cho M-002
        print("\n--- [GIAI DOAN 2: TIEM LOI & PHAT HIEN SU CO] ---")
        now_utc = datetime.now(timezone.utc)
        fault_record = {
            "deviceId": "M-002",
            "type": "air_leak",
            "severity": 0.9,
            "t0": now_utc,
            "offset_sec": 0,
            "createdAt": now_utc,
            "resolved": False,
        }
        db["fault_labels"].insert_one(fault_record)
        print("[*] Da ghi nhan tiem loi 'air_leak' (severity=0.9) cho M-002 vao MongoDB.")

        # Cho simulator dong bo loi va gateway tinh cua so moi (polling toi da 15s)
        active_incidents = []
        m2_state = {}
        for _ in range(15):
            time.sleep(1.0)
            active_incidents = list(db["incidents"].find({"deviceId": "M-002", "status": "active"}))
            m2_state = db["device_state"].find_one({"deviceId": "M-002"}) or {}
            if active_incidents:
                break

        print(f"[*] Trang thai hien tai cua M-002: status={m2_state.get('status')}, HI={m2_state.get('hi')}, RUL={m2_state.get('rulHours')}h")
        print(f"[*] So incident active cua M-002: {len(active_incidents)}")

        assert len(active_incidents) >= 1, "Loi: Streaming Engine chua tu dong mo incident cho M-002 khi suy giam!"
        print("[PASS] Giai doan 2: Streaming Engine da phat hien suy thoai va tu dong mo incident thanh cong.")

        # BƯỚC 3: Chạy Scheduler và kiểm tra tính Idempotent
        print("\n--- [GIAI DOAN 3: LAP LICH BAO TRI (SCHEDULER & IDEMPOTENCY)] ---")
        # Override tam MONGO_URI va DB_NAME cho scheduler
        import scheduler.app as sched_app
        sched_app.MONGO_URI = TEST_MONGO_URI
        sched_app.DB_NAME = TEST_DB_NAME

        res1 = run_greedy_scheduler(capacity=2, horizon_hours=72.0)
        print(f"[*] Scheduler lan 1: scheduled={res1['scheduled_count']}, skipped={res1['skipped_active_count']}")
        orders_1 = list(db["work_orders"].find({"deviceId": "M-002", "status": "scheduled"}))
        assert len(orders_1) == 1, "Loi: Chua tao duoc work order cho M-002!"

        # Chay lai lan 2 ngay lap tuc -> M-002 phai bi skipped de khong trung lap
        res2 = run_greedy_scheduler(capacity=2, horizon_hours=72.0)
        print(f"[*] Scheduler lan 2: scheduled={res2['scheduled_count']}, skipped={res2['skipped_active_count']}")
        orders_2 = list(db["work_orders"].find({"deviceId": "M-002", "status": "scheduled"}))
        assert len(orders_2) == 1, "Loi: Idempotency that bai, da tao trung lenh bao tri cho M-002!"
        print("[PASS] Giai doan 3: Scheduler lap lich thanh cong va bao dam Idempotency tuyet doi.")

        # BƯỚC 4: Phục hồi sự cố và kiểm tra đóng Incident tự động
        print("\n--- [GIAI DOAN 4: DONG INCIDENT KHI THIET BI PHUC HOI VE OK] ---")
        # Giai quyet loi trong fault_labels
        db["fault_labels"].update_many({"deviceId": "M-002"}, {"$set": {"resolved": True}})
        print("[*] Da danh dau resolved=True cho loi cua M-002.")

        # Cho simulator dong bo va phat du lieu khoe manh tro lai (polling toi da 20 giay)
        resolved_incidents = []
        for step in range(20):
            time.sleep(1.0)
            resolved_incidents = list(db["incidents"].find({"deviceId": "M-002", "status": "resolved"}))
            m2_state = db["device_state"].find_one({"deviceId": "M-002"}) or {}
            edge = m2_state.get("edge", {})
            print(f"  [Loop {step+1:02d}s] M-002: status={m2_state.get('status')}, HI={m2_state.get('hi')}, cusum_pos={edge.get('cusum_pos')}, cusum_alert={edge.get('cusum_alert')}")
            if resolved_incidents:
                break

        still_active = list(db["incidents"].find({"deviceId": "M-002", "status": "active"}))
        print(f"[*] So incident da resolved cua M-002: {len(resolved_incidents)}")
        print(f"[*] So incident con active cua M-002: {len(still_active)}")

        assert len(resolved_incidents) >= 1, "Loi: Incident chua duoc dong khi thiet bi phuc hoi!"
        print("[PASS] Giai doan 4: Vong doi Incident hoan tat, tu dong resolved thanh cong.")

        print("\n" + "=" * 70)
        print("[SUCCESS] TOAN BO HE THONG FLEETPULSE HOAT DONG CHUAN XAC 100% TREN DOCKER!")
        print("=" * 70)

    finally:
        # Don dep cac service
        ingestion.client.loop_stop()
        gateway.client.loop_stop()
        simulator.client.loop_stop()
        engine.running = False
        client.close()


if __name__ == "__main__":
    run_live_test()
