"""Kiểm tra kết nối MongoDB cho FleetPulse.

Kiểm tra: ping, phiên bản >= 7, chạy ở chế độ replica set (bắt buộc cho
Change Streams), và các collection đã được tạo bởi mongo/setup.js.
Dùng được cho cả MongoDB cục bộ (Docker) lẫn Atlas, chỉ cần đổi MONGODB_URI.
"""

import os
import sys

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

REQUIRED_COLLECTIONS = [
    "telemetry",
    "features_live",
    "device_state",
    "incidents",
    "work_orders",
    "fleet_rollup_1m",
]
MIN_MAJOR_VERSION = 7

load_dotenv()

uri = os.getenv("MONGODB_URI")
database_name = os.getenv("MONGODB_DATABASE", "fleetpulse_dev")

if not uri:
    raise ValueError("Chua cau hinh MONGODB_URI trong file .env")


def main() -> int:
    ok = True
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    try:
        client.admin.command("ping")
        print("[OK] Ket noi MongoDB thanh cong")

        version = client.server_info()["version"]
        major = int(version.split(".")[0])
        if major >= MIN_MAJOR_VERSION:
            print(f"[OK] Phien ban MongoDB: {version}")
        else:
            print(f"[LOI] Phien ban MongoDB {version} < {MIN_MAJOR_VERSION}")
            ok = False

        hello = client.admin.command("hello")
        set_name = hello.get("setName")
        if set_name:
            print(f"[OK] Replica set: {set_name} (Change Streams dung duoc)")
        else:
            print("[LOI] Khong chay replica set -> Change Streams se khong hoat dong")
            ok = False

        db = client[database_name]
        print(f"     Database: {database_name}")
        infos = {c["name"]: c for c in db.list_collections()}
        missing = [name for name in REQUIRED_COLLECTIONS if name not in infos]
        if missing:
            print(f"[LOI] Thieu collection: {', '.join(missing)} -> chay mongo/setup.js")
            ok = False
        else:
            print(f"[OK] Du {len(REQUIRED_COLLECTIONS)} collection bat buoc")

        telemetry = infos.get("telemetry")
        if telemetry is not None:
            if telemetry.get("type") == "timeseries":
                print("[OK] telemetry la Time Series Collection")
            else:
                print("[LOI] telemetry khong phai Time Series Collection")
                ok = False
    except PyMongoError as exc:
        print(f"[LOI] {exc}")
        ok = False
    finally:
        client.close()

    print("==> TAT CA DEU OK" if ok else "==> CO LOI, xem cac dong [LOI] o tren")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
