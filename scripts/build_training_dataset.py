"""Script trích xuất đặc trưng lịch sử theo batch, gán nhãn Ground Truth và chia Train/Val/Test.

Đầu vào: data/processed/metropt3.parquet
Đầu ra:
  data/ai/train_features.parquet
  data/ai/val_features.parquet
  data/ai/test_features.parquet
  data/ai/dataset_metadata.json
"""

from __future__ import annotations

import argparse
import json
import logging
import math
from datetime import datetime, timezone
import sys
from pathlib import Path

# Đảm bảo UTF-8 trên Windows console
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from fp_algo.contract import FEATURE_ORDER
from fp_algo.features import window_features

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("DatasetBuilder")

ROOT = Path(__file__).resolve().parents[1]
INPUT_PARQUET = ROOT / "data" / "processed" / "metropt3.parquet"
OUTPUT_DIR = ROOT / "data" / "ai"

# Các khoảng thời gian sự cố rò khí được ghi nhận (Davari et al., 2021)
REPORTED_AIR_LEAKS = [
    ("2020-04-18 00:00:00", "2020-04-18 23:59:59"),
    ("2020-05-29 23:30:00", "2020-05-30 06:00:00"),
    ("2020-06-05 10:00:00", "2020-06-07 14:30:00"),
    ("2020-07-15 14:30:00", "2020-07-15 19:00:00"),
]

# Ranh giới phân chia Train / Val / Test theo thời gian (Time-based Cutoffs)
# Mỗi tập đều chứa sự cố thực tế để đánh giá mô hình khách quan
SPLIT_CUTOFF_TRAIN_VAL = pd.Timestamp("2020-06-01 00:00:00")
SPLIT_CUTOFF_VAL_TEST = pd.Timestamp("2020-07-01 00:00:00")


def load_raw_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file {path}")
    logger.info(f"Đang đọc dữ liệu từ {path}...")
    df = pd.read_parquet(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    logger.info(f"Đã đọc {len(df):,} dòng. Thời gian: {df['timestamp'].min()} -> {df['timestamp'].max()}")
    return df


def parse_leak_intervals() -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    return [(pd.Timestamp(s), pd.Timestamp(e)) for s, e in REPORTED_AIR_LEAKS]


def compute_labels(
    ts: pd.Timestamp,
    intervals: list[tuple[pd.Timestamp, pd.Timestamp]],
) -> dict[str, Any]:
    """Gán nhãn anomaly và RUL có xử lý censoring theo nguyên tắc time-series."""
    # 1. Kiểm tra xem timestamp có nằm trong khoảng sự cố không
    for start_t, end_t in intervals:
        if start_t <= ts <= end_t:
            return {
                "label_anomaly": 1,
                "label_valid": True,
                "rul_hours": 0.0,
                "rul_valid": True,
                "event_type": "in_failure",
            }

    # 2. Tìm sự cố tương lai gần nhất
    future_starts = [start_t for start_t, _ in intervals if start_t > ts]

    if future_starts:
        next_failure = min(future_starts)
        diff_hours = (next_failure - ts).total_seconds() / 3600.0
        return {
            "label_anomaly": 0,
            "label_valid": True,
            "rul_hours": round(diff_hours, 2),
            "rul_valid": True,
            "event_type": "normal_run_to_failure",
        }
    else:
        # Không có sự cố tương lai nào được quan sát (Right-Censored)
        # Bắt buộc KHÔNG được gán rul_hours = 0
        return {
            "label_anomaly": 0,
            "label_valid": True,
            "rul_hours": np.nan,
            "rul_valid": False,
            "event_type": "right_censored",
        }


def extract_features_batch(
    df: pd.DataFrame,
    window_samples: int = 6,
    step_samples: int = 6,
    max_gap_seconds: float = 60.0,
) -> pd.DataFrame:
    """Trích xuất feature dùng chung logic từ fp_algo.features."""
    intervals = parse_leak_intervals()
    records = []
    dropped_gaps = 0

    # Chuyển dataframe thành mảng dict để trích xuất nhanh
    columns = ["timestamp", "TP2", "TP3", "Motor_current", "Oil_temperature", "COMP"]
    data_dict = {col: df[col].values for col in columns if col in df.columns}
    total_len = len(df)

    logger.info(f"Bắt đầu trích xuất đặc trưng (cửa sổ={window_samples} mẫu, bước={step_samples} mẫu)...")

    for i in range(0, total_len - window_samples + 1, step_samples):
        # Kiểm tra khoảng trống thời gian trong cửa sổ (time gap)
        ts_start = pd.Timestamp(data_dict["timestamp"][i])
        ts_end = pd.Timestamp(data_dict["timestamp"][i + window_samples - 1])
        elapsed = (ts_end - ts_start).total_seconds()

        # Nếu độ dài thực tế vượt quá ngưỡng dự kiến (có khoảng trống mất dữ liệu)
        expected_span = (window_samples - 1) * 10.0
        if elapsed > expected_span + max_gap_seconds:
            dropped_gaps += 1
            continue

        # Gom mẫu cho cửa sổ
        samples = []
        for j in range(i, i + window_samples):
            samples.append(
                {
                    "TP2": data_dict["TP2"][j],
                    "TP3": data_dict["TP3"][j],
                    "Motor_current": data_dict["Motor_current"][j],
                    "Oil_temperature": data_dict["Oil_temperature"][j],
                    "COMP": data_dict["COMP"][j],
                    "ts": data_dict["timestamp"][j],
                }
            )

        # Tính đặc trưng bằng hàm chuẩn dùng chung
        feat = window_features(samples)

        # Gán nhãn cho mốc thời gian kết thúc cửa sổ
        labels = compute_labels(ts_end, intervals)

        row = {
            "window_start": ts_start,
            "window_end": ts_end,
            "timestamp": ts_end,
            **feat,
            **labels,
        }
        records.append(row)

    logger.info(f"Đã tạo {len(records):,} dòng đặc trưng (Bỏ qua {dropped_gaps:,} cửa sổ do đứt đoạn thời gian).")
    feat_df = pd.DataFrame(records)
    return feat_df


def split_and_save(feat_df: pd.DataFrame, out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Chia theo mốc thời gian tuyệt đối (Time-series split)
    train_mask = feat_df["timestamp"] < SPLIT_CUTOFF_TRAIN_VAL
    val_mask = (feat_df["timestamp"] >= SPLIT_CUTOFF_TRAIN_VAL) & (feat_df["timestamp"] < SPLIT_CUTOFF_VAL_TEST)
    test_mask = feat_df["timestamp"] >= SPLIT_CUTOFF_VAL_TEST

    df_train = feat_df[train_mask].copy().reset_index(drop=True)
    df_val = feat_df[val_mask].copy().reset_index(drop=True)
    df_test = feat_df[test_mask].copy().reset_index(drop=True)

    # Lưu Parquet
    p_train = out_dir / "train_features.parquet"
    p_val = out_dir / "val_features.parquet"
    p_test = out_dir / "test_features.parquet"

    df_train.to_parquet(p_train, index=False)
    df_val.to_parquet(p_val, index=False)
    df_test.to_parquet(p_test, index=False)

    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "feature_order": FEATURE_ORDER,
        "splits": {
            "train": {
                "file": str(p_train.name),
                "rows": int(len(df_train)),
                "start": str(df_train["timestamp"].min()),
                "end": str(df_train["timestamp"].max()),
                "anomaly_count": int(df_train["label_anomaly"].sum()),
                "anomaly_ratio": round(float(df_train["label_anomaly"].mean()), 4),
                "valid_rul_count": int(df_train["rul_valid"].sum()),
            },
            "val": {
                "file": str(p_val.name),
                "rows": int(len(df_val)),
                "start": str(df_val["timestamp"].min()),
                "end": str(df_val["timestamp"].max()),
                "anomaly_count": int(df_val["label_anomaly"].sum()),
                "anomaly_ratio": round(float(df_val["label_anomaly"].mean()), 4),
                "valid_rul_count": int(df_val["rul_valid"].sum()),
            },
            "test": {
                "file": str(p_test.name),
                "rows": int(len(df_test)),
                "start": str(df_test["timestamp"].min()),
                "end": str(df_test["timestamp"].max()),
                "anomaly_count": int(df_test["label_anomaly"].sum()),
                "anomaly_ratio": round(float(df_test["label_anomaly"].mean()), 4),
                "valid_rul_count": int(df_test["rul_valid"].sum()),
            },
        },
    }

    meta_file = out_dir / "dataset_metadata.json"
    meta_file.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"Đã lưu siêu dữ liệu tập dataset tại {meta_file}")
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description="FleetPulse AI Dataset Builder")
    parser.add_argument("--input", default=str(INPUT_PARQUET), help="File Parquet đã tiền xử lý")
    parser.add_argument("--out-dir", default=str(OUTPUT_DIR), help="Thư mục xuất file Parquet cho AI")
    parser.add_argument("--window-samples", type=int, default=6, help="Số mẫu trong 1 cửa sổ (mặc định 6 mẫu ~ 60s)")
    parser.add_argument("--step-samples", type=int, default=6, help="Bước trượt cửa sổ (mặc định 6 mẫu để không chồng lấn)")
    args = parser.parse_args()

    raw_df = load_raw_dataset(Path(args.input))
    feat_df = extract_features_batch(
        raw_df,
        window_samples=args.window_samples,
        step_samples=args.step_samples,
    )
    meta = split_and_save(feat_df, Path(args.out_dir))

    print("\n" + "=" * 60)
    print("HOÀN THÀNH TẠO DATASET CHO AI:")
    for split_name, s in meta["splits"].items():
        print(f" - {split_name.upper():<6}: {s['rows']:,} dòng | Bất thường={s['anomaly_count']:,} ({s['anomaly_ratio']:.2%}) | RUL hợp lệ={s['valid_rul_count']:,}")
        print(f"           Thời gian: {s['start']} -> {s['end']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
