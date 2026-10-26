"""Kiểm thử kết nối MongoDB, Replica Set, Index và Change Streams (Local và Cloud)."""

import os
import time
import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError

from mongo.client import (
    DEFAULT_LOCAL_URI,
    get_mongo_database_name,
    get_mongo_uri,
    sanitize_uri,
)


def test_local_mongodb_ping_and_replicaset() -> None:
    """Kiểm tra MongoDB Local (Docker) kết nối được và đang chạy replica set rs0."""
    local_uri = os.getenv("MONGODB_LOCAL_URI") or DEFAULT_LOCAL_URI
    db_name = get_mongo_database_name()

    try:
        client = MongoClient(local_uri, serverSelectionTimeoutMS=4000)
        client.admin.command("ping")
        hello = client.admin.command("hello")
        rs_name = hello.get("setName")
        assert rs_name is not None, "Local MongoDB phải chạy ở chế độ replica set để hỗ trợ Change Streams"
        assert rs_name == "rs0"
    except (ServerSelectionTimeoutError, PyMongoError) as e:
        pytest.fail(f"Không thể kết nối MongoDB Local tại {local_uri}: {e}")
    finally:
        client.close()


def test_local_change_streams_event() -> None:
    """Kiểm tra tính năng Change Streams trên collection features_live của Local MongoDB."""
    local_uri = os.getenv("MONGODB_LOCAL_URI") or DEFAULT_LOCAL_URI
    db_name = get_mongo_database_name()

    client = MongoClient(local_uri, serverSelectionTimeoutMS=4000)
    db = client[db_name]
    col = db["features_live"]

    test_doc = {
        "deviceId": "TEST-DEV-001",
        "windowStart": f"TEST-WINDOW-{time.time()}",
        "feat": {"tp2_mean": 8.9},
        "shard": 0,
    }

    try:
        with col.watch(max_await_time_ms=2000) as stream:
            col.insert_one(test_doc)
            change = stream.next()
            assert change["operationType"] == "insert"
            assert change["fullDocument"]["deviceId"] == "TEST-DEV-001"
    finally:
        col.delete_one({"deviceId": "TEST-DEV-001"})
        client.close()


def test_cloud_mongodb_connectivity_or_blocked() -> None:
    """Kiểm tra MongoDB Cloud (Atlas)."""
    cloud_uri = get_mongo_uri("cloud")
    if not cloud_uri or "localhost" in cloud_uri or "127.0.0.1" in cloud_uri:
        pytest.skip("Chưa cấu hình MONGODB_URI dạng Cloud trong .env")

    safe_uri = sanitize_uri(cloud_uri)
    print(f"\n[*] Kiểm tra kết nối MongoDB Cloud: {safe_uri}")

    client = MongoClient(cloud_uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
    try:
        client.admin.command("ping")
        hello = client.admin.command("hello")
        assert hello.get("setName") is not None
        print(f"[OK] Kết nối Cloud thành công. ReplicaSet={hello.get('setName')}")
    except Exception as exc:
        err_str = str(exc)
        if "TLSV1_ALERT_INTERNAL_ERROR" in err_str or "timed out" in err_str:
            pytest.skip(
                f"[BLOCKED] MongoDB Cloud chưa cho phép IP hiện tại truy cập. "
                f"Vui lòng thêm IP vào Atlas Network Access (IP Allowlist). Chi tiết lỗi: {err_str}"
            )
        else:
            pytest.fail(f"Lỗi kết nối MongoDB Cloud ({safe_uri}): {exc}")
    finally:
        client.close()


def test_cloud_change_streams_event() -> None:
    """Kiểm tra Change Streams trên MongoDB Cloud Atlas."""
    cloud_uri = get_mongo_uri("cloud")
    if not cloud_uri or "localhost" in cloud_uri or "127.0.0.1" in cloud_uri:
        pytest.skip("Chưa cấu hình MONGODB_URI dạng Cloud trong .env")

    db_name = get_mongo_database_name()
    client = MongoClient(cloud_uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
    try:
        db = client[db_name]
        col = db["features_live"]

        test_doc = {
            "deviceId": "CLOUD-TEST-002",
            "windowStart": f"TEST-WINDOW-{time.time()}",
            "feat": {"tp2_mean": 8.9},
            "shard": 0,
        }

        with col.watch(max_await_time_ms=3000) as stream:
            col.insert_one(test_doc)
            change = stream.next()
            assert change["operationType"] == "insert"
            assert change["fullDocument"]["deviceId"] == "CLOUD-TEST-002"
    except Exception as exc:
        err_str = str(exc)
        if "TLSV1_ALERT_INTERNAL_ERROR" in err_str or "timed out" in err_str:
            pytest.skip(f"[BLOCKED] Cloud chưa mở IP Network Access: {err_str}")
        else:
            pytest.fail(f"Lỗi Change Stream Cloud: {exc}")
    finally:
        try:
            col.delete_one({"deviceId": "CLOUD-TEST-002"})
        except Exception:
            pass
        client.close()
