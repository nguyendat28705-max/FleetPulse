"""Huấn luyện và đóng gói mô hình AI (Health Index & RUL) cho FleetPulse.

Đầu vào:
    data/ai/train_features.parquet
    data/ai/val_features.parquet
    data/ai/test_features.parquet
Đầu ra:
    models/fleetpulse_model.joblib
    models/model_metadata.json
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Đảm bảo UTF-8 an toàn trên Windows console
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fp_algo.contract import CONTRACT_VERSION, FEATURE_ORDER

DATA_DIR = ROOT / "data" / "ai"
MODELS_DIR = ROOT / "models"


def make_hi_target(df: pd.DataFrame) -> np.ndarray:
    """Tạo nhãn mục tiêu Health Index (HI):
    - Khỏe mạnh: ~1.0
    - Suy thoái trước sự cố (<24h): tăng dần từ 1.0 đến 6.0
    - Đang xảy ra sự cố (label_anomaly == 1): ~7.5
    """
    rul_safe = np.nan_to_num(df["rul_hours"].to_numpy(dtype=float), nan=999.0)
    anomaly = df["label_anomaly"].to_numpy(dtype=float)
    degrade_ramp = np.maximum(0.0, 5.0 * (1.0 - rul_safe / 24.0))
    return np.where(anomaly == 1.0, 7.5, 1.0 + degrade_ramp)


def train_and_export() -> None:
    print("=" * 65)
    print("    FleetPulse: Huấn luyện Mô hình AI (HI & RUL Regressors)")
    print("=" * 65)

    train_path = DATA_DIR / "train_features.parquet"
    val_path = DATA_DIR / "val_features.parquet"
    test_path = DATA_DIR / "test_features.parquet"

    for p in (train_path, val_path, test_path):
        if not p.exists():
            print(f"[!] Lỗi: Không tìm thấy file {p.name} tại {p}", file=sys.stderr)
            print("    Vui lòng chạy 'python scripts/build_training_dataset.py' trước.", file=sys.stderr)
            sys.exit(1)

    # 1. Đọc dữ liệu
    print("[1/5] Đang nạp các tập dữ liệu Parquet...")
    t_start = time.time()
    train_df = pd.read_parquet(train_path)
    val_df = pd.read_parquet(val_path)
    test_df = pd.read_parquet(test_path)
    print(f"  -> Train: {len(train_df):,} dòng | Val: {len(val_df):,} dòng | Test: {len(test_df):,} dòng ({time.time() - t_start:.2f}s)")

    X_train = train_df[FEATURE_ORDER]
    X_val = val_df[FEATURE_ORDER]
    X_test = test_df[FEATURE_ORDER]

    X_train_np = X_train.to_numpy()
    X_val_np = X_val.to_numpy()
    X_test_np = X_test.to_numpy()

    # 2. Huấn luyện Model 1: Health Index (HI)
    print("\n[2/5] Huấn luyện Mô hình Health Index (HistGradientBoostingRegressor)...")
    y_hi_train = make_hi_target(train_df)
    y_hi_val = make_hi_target(val_df)
    y_hi_test = make_hi_target(test_df)

    t0 = time.time()
    hi_model = HistGradientBoostingRegressor(
        max_iter=120,
        learning_rate=0.08,
        max_leaf_nodes=31,
        random_state=42,
    )
    hi_model.fit(X_train_np, y_hi_train)
    hi_train_time = time.time() - t0
    print(f"  [OK] Đã huấn luyện xong HI Model trong {hi_train_time:.2f} giây.")

    val_hi_pred = hi_model.predict(X_val_np)
    test_hi_pred = hi_model.predict(X_test_np)

    val_hi_mae = float(mean_absolute_error(y_hi_val, val_hi_pred))
    test_hi_mae = float(mean_absolute_error(y_hi_test, test_hi_pred))
    print(f"  -> HI Validation MAE: {val_hi_mae:.4f}")
    print(f"  -> HI Test MAE:       {test_hi_mae:.4f}")

    # 3. Huấn luyện Model 2: Remaining Useful Life (RUL)
    print("\n[3/5] Huấn luyện Mô hình RUL (Remaining Useful Life)...")
    # Lọc bỏ các mẫu right-censored không có ground-truth RUL
    train_rul_mask = train_df["rul_valid"].to_numpy()
    val_rul_mask = val_df["rul_valid"].to_numpy()
    test_rul_mask = test_df["rul_valid"].to_numpy()

    MAX_RUL_CAP = 200.0  # Giới hạn trần piecewise linear RUL (200 giờ)
    y_rul_train = np.clip(train_df.loc[train_rul_mask, "rul_hours"].to_numpy(dtype=float), 0.0, MAX_RUL_CAP)
    y_rul_val = np.clip(val_df.loc[val_rul_mask, "rul_hours"].to_numpy(dtype=float), 0.0, MAX_RUL_CAP)
    y_rul_test = np.clip(test_df.loc[test_rul_mask, "rul_hours"].to_numpy(dtype=float), 0.0, MAX_RUL_CAP)

    X_rul_train_np = X_train_np[train_rul_mask]
    X_rul_val_np = X_val_np[val_rul_mask]
    X_rul_test_np = X_test_np[test_rul_mask]

    t1 = time.time()
    rul_model = HistGradientBoostingRegressor(
        max_iter=120,
        learning_rate=0.08,
        max_leaf_nodes=31,
        random_state=42,
    )
    rul_model.fit(X_rul_train_np, y_rul_train)
    rul_train_time = time.time() - t1
    print(f"  [OK] Đã huấn luyện xong RUL Model trong {rul_train_time:.2f} giây.")

    val_rul_pred = rul_model.predict(X_rul_val_np)
    test_rul_pred = rul_model.predict(X_rul_test_np)

    val_rul_mae = float(mean_absolute_error(y_rul_val, val_rul_pred))
    test_rul_mae = float(mean_absolute_error(y_rul_test, test_rul_pred))
    print(f"  -> RUL Validation MAE: {val_rul_mae:.2f} giờ")
    print(f"  -> RUL Test MAE:       {test_rul_mae:.2f} giờ")

    # 4. Đóng gói Artifact theo Contract FleetPulse
    print("\n[4/5] Đóng gói mô hình thành artifact chuẩn...")
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    artifact_path = MODELS_DIR / "fleetpulse_model.joblib"

    bundle = {
        "hi_model": hi_model,
        "rul_model": rul_model,
        "feature_order": FEATURE_ORDER,
        "contract_version": CONTRACT_VERSION,
        "max_rul_cap": MAX_RUL_CAP,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "val_hi_mae": round(val_hi_mae, 4),
            "test_hi_mae": round(test_hi_mae, 4),
            "val_rul_mae_hours": round(val_rul_mae, 2),
            "test_rul_mae_hours": round(test_rul_mae, 2),
        },
    }
    joblib.dump(bundle, artifact_path)
    file_size_kb = artifact_path.stat().st_size / 1024
    print(f"  [OK] Đã lưu artifact tại: {artifact_path} ({file_size_kb:.1f} KB)")

    # Lưu metadata json để kiểm toán và báo cáo
    meta_path = MODELS_DIR / "model_metadata.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "contract_version": CONTRACT_VERSION,
                "trained_at": bundle["trained_at"],
                "feature_order": FEATURE_ORDER,
                "total_train_samples": int(len(train_df)),
                "total_val_samples": int(len(val_df)),
                "total_test_samples": int(len(test_df)),
                "training_duration_seconds": round(hi_train_time + rul_train_time, 2),
                "metrics": bundle["metrics"],
            },
            f,
            indent=2,
            ensure_ascii=False,
        )
    print(f"  [OK] Đã lưu metadata kiểm toán tại: {meta_path}")

    # 5. Tổng kết
    print("\n[5/5] Hoàn thành Bước 2!")
    print("=" * 65)
    print(f"Tổng thời gian huấn luyện: {time.time() - t_start:.2f} giây.")
    print("Mô hình AI đã sẵn sàng đưa vào vận hành thời gian thực.")
    print("=" * 65)


if __name__ == "__main__":
    train_and_export()
