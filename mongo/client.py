"""Quản lý kết nối MongoDB tập trung cho FleetPulse (hỗ trợ chuyển đổi Local / Cloud)."""

from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import urlparse

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError

load_dotenv()

DEFAULT_LOCAL_URI = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
DEFAULT_DB_NAME = "fleetpulse_dev"


def sanitize_uri(uri: str) -> str:
    """Che mật khẩu trong URI trước khi ghi log hoặc hiển thị console."""
    if not uri:
        return ""
    # Mask password in mongodb://user:pass@host or mongodb+srv://user:pass@host
    return re.sub(r":([^/@:]+)@", ":****@", uri)


def get_mongo_mode() -> str:
    """Xác định chế độ MongoDB: 'cloud' hoặc 'local'."""
    mode = os.getenv("MONGODB_MODE", "").strip().lower()
    if mode in ("cloud", "atlas"):
        return "cloud"
    if mode in ("local", "docker"):
        return "local"

    # Tự động nhận diện nếu MONGODB_URI là srv hoặc chứa atlas
    uri = os.getenv("MONGODB_URI", "")
    if uri.startswith("mongodb+srv://") or "mongodb.net" in uri:
        return "cloud"
    return "local"


def get_mongo_uri(mode: str | None = None) -> str:
    """Lấy MongoDB URI theo mode (local hoặc cloud)."""
    target_mode = mode or get_mongo_mode()
    if target_mode == "cloud":
        cloud_uri = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
        if cloud_uri:
            return cloud_uri

    # Local fallback
    local_uri = os.getenv("MONGODB_LOCAL_URI") or DEFAULT_LOCAL_URI
    return local_uri


def get_mongo_database_name() -> str:
    """Lấy tên database MongoDB."""
    return os.getenv("MONGODB_DATABASE") or os.getenv("MONGO_DB") or DEFAULT_DB_NAME


def create_mongo_client(
    uri: str | None = None,
    timeout_ms: int = 5000,
    retry_writes: bool = True,
    max_pool_size: int = 50,
) -> MongoClient:
    """Tạo kết nối MongoClient an toàn với cấu hình timeout và connection pool chuẩn."""
    target_uri = uri or get_mongo_uri()
    client = MongoClient(
        target_uri,
        serverSelectionTimeoutMS=timeout_ms,
        connectTimeoutMS=timeout_ms,
        retryWrites=retry_writes,
        maxPoolSize=max_pool_size,
    )
    return client
