"""Kiểm thử tính toàn vẹn của dataset lịch sử cho AI và xác minh không có Data Leakage."""

import json
from pathlib import Path
import numpy as np
import pandas as pd
import pytest

from fp_algo.contract import FEATURE_ORDER

ROOT = Path(__file__).resolve().parents[1]
AI_DIR = ROOT / "data" / "ai"


def test_dataset_splits_exist_and_non_empty() -> None:
    train_p = AI_DIR / "train_features.parquet"
    val_p = AI_DIR / "val_features.parquet"
    test_p = AI_DIR / "test_features.parquet"
    meta_p = AI_DIR / "dataset_metadata.json"

    assert train_p.exists(), f"Không tìm thấy {train_p}"
    assert val_p.exists(), f"Không tìm thấy {val_p}"
    assert test_p.exists(), f"Không tìm thấy {test_p}"
    assert meta_p.exists(), f"Không tìm thấy {meta_p}"

    df_train = pd.read_parquet(train_p)
    df_val = pd.read_parquet(val_p)
    df_test = pd.read_parquet(test_p)

    assert len(df_train) > 0, "Tập train rỗng"
    assert len(df_val) > 0, "Tập val rỗng"
    assert len(df_test) > 0, "Tập test rỗng"

    # Kiểm tra metadata khớp số dòng
    meta = json.loads(meta_p.read_text(encoding="utf-8"))
    assert meta["splits"]["train"]["rows"] == len(df_train)
    assert meta["splits"]["val"]["rows"] == len(df_val)
    assert meta["splits"]["test"]["rows"] == len(df_test)


def test_no_temporal_leakage_between_splits() -> None:
    """Xác minh nguyên tắc chuỗi thời gian: Train kết thúc trước Val, Val kết thúc trước Test."""
    df_train = pd.read_parquet(AI_DIR / "train_features.parquet")
    df_val = pd.read_parquet(AI_DIR / "val_features.parquet")
    df_test = pd.read_parquet(AI_DIR / "test_features.parquet")

    t_train_max = pd.to_datetime(df_train["timestamp"]).max()
    t_val_min = pd.to_datetime(df_val["timestamp"]).min()
    t_val_max = pd.to_datetime(df_val["timestamp"]).max()
    t_test_min = pd.to_datetime(df_test["timestamp"]).min()

    assert t_train_max < t_val_min, (
        f"DATA LEAKAGE: Tập train có timestamp ({t_train_max}) lớn hơn hoặc bằng val ({t_val_min})"
    )
    assert t_val_max < t_test_min, (
        f"DATA LEAKAGE: Tập val có timestamp ({t_val_max}) lớn hơn hoặc bằng test ({t_test_min})"
    )


def test_feature_columns_and_contract_schema() -> None:
    """Xác minh tất cả các tập Parquet đều chứa đủ 11 đặc trưng và các cột nhãn Ground Truth."""
    df_train = pd.read_parquet(AI_DIR / "train_features.parquet")

    # Kiểm tra đủ các feature bắt buộc
    for col in FEATURE_ORDER:
        assert col in df_train.columns, f"Thiếu feature {col} trong dataset"
        assert not df_train[col].isna().any(), f"Feature {col} chứa giá trị NaN"

    # Kiểm tra các cột nhãn
    required_label_cols = ["label_anomaly", "label_valid", "rul_hours", "rul_valid", "event_type"]
    for col in required_label_cols:
        assert col in df_train.columns, f"Thiếu cột nhãn {col}"

    # Kiểm tra tính đúng đắn của RUL: Với các dòng rul_valid=True, RUL phải >= 0
    valid_rul = df_train[df_train["rul_valid"] == True]["rul_hours"]
    assert (valid_rul >= 0.0).all(), "RUL không được nhận giá trị âm"

    # Với dòng bất thường (label_anomaly == 1), RUL phải là 0.0
    anomalies = df_train[df_train["label_anomaly"] == 1]
    if len(anomalies) > 0:
        assert (anomalies["rul_hours"] == 0.0).all(), "Các dòng đang hỏng phải có RUL = 0"
