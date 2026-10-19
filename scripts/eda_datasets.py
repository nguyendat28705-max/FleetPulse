"""Khám phá nhanh MetroPT-3 và SKAB, chuyển MetroPT-3 sang Parquet cho simulator.

Chạy:  python scripts/eda_datasets.py
Đầu vào:  data/raw/metropt3.zip (UCI 791), data/raw/SKAB/ (git clone waico/SKAB)
Đầu ra:   data/processed/metropt3.parquet, data/processed/eda_summary.json
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "processed"

# Các lần hỏng do rò khí theo mô tả dataset (Davari et al., 2021) - CẦN đối chiếu lại trang UCI.
REPORTED_AIR_LEAKS = [
    ("2020-04-18 00:00", "2020-04-18 23:59"),
    ("2020-05-29 23:30", "2020-05-30 06:00"),
    ("2020-06-05 10:00", "2020-06-07 14:30"),
    ("2020-07-15 14:30", "2020-07-15 19:00"),
]


def find_metropt_csv() -> Path:
    direct = RAW / "MetroPT3(AirCompressor).csv"
    if direct.exists():
        return direct
    extract_dir = RAW / "metropt3"
    csvs = list(extract_dir.rglob("*.csv")) if extract_dir.exists() else []
    if not csvs:
        zip_path = RAW / "metropt3.zip"
        if not zip_path.exists():
            raise FileNotFoundError(f"Khong thay {zip_path}")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(extract_dir)
        # UCI đôi khi lồng thêm một file zip bên trong
        for inner in list(extract_dir.rglob("*.zip")):
            with zipfile.ZipFile(inner) as zf:
                zf.extractall(inner.parent)
        csvs = list(extract_dir.rglob("*.csv"))
    if not csvs:
        raise FileNotFoundError("Khong tim thay file CSV MetroPT-3 sau khi giai nen")
    return max(csvs, key=lambda p: p.stat().st_size)


def eda_metropt() -> dict:
    csv_path = find_metropt_csv()
    print(f"\n=== MetroPT-3: {csv_path.relative_to(ROOT)} ({csv_path.stat().st_size / 1e6:.1f} MB)")
    df = pd.read_csv(csv_path)
    df = df.drop(columns=[c for c in df.columns if c.startswith("Unnamed")])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    dt = df["timestamp"].diff().dt.total_seconds().dropna()
    gaps = dt[dt > 60]
    summary = {
        "file": str(csv_path.relative_to(ROOT)),
        "rows": int(len(df)),
        "columns": [c for c in df.columns],
        "start": str(df["timestamp"].min()),
        "end": str(df["timestamp"].max()),
        "dt_seconds": {
            "median": float(dt.median()),
            "mean": float(dt.mean()),
            "p05": float(dt.quantile(0.05)),
            "p95": float(dt.quantile(0.95)),
            "top_values": {str(k): int(v) for k, v in dt.round().value_counts().head(5).items()},
        },
        "gaps_over_60s": int(len(gaps)),
        "largest_gap_hours": float(gaps.max() / 3600) if len(gaps) else 0.0,
        "comp_value_counts": {str(k): int(v) for k, v in df["COMP"].value_counts().items()}
        if "COMP" in df
        else None,
    }

    print(f"So dong: {summary['rows']:,}")
    print(f"Khoang thoi gian: {summary['start']} -> {summary['end']}")
    print(f"Cot: {summary['columns']}")
    print(f"Khoang cach mau (s): median={dt.median():.2f}, mean={dt.mean():.2f}, "
          f"p05={dt.quantile(0.05):.2f}, p95={dt.quantile(0.95):.2f}")
    print(f"Gia tri khoang cach pho bien: {summary['dt_seconds']['top_values']}")
    print(f"So khoang trong > 60s: {summary['gaps_over_60s']}, lon nhat {summary['largest_gap_hours']:.1f} gio")
    print(f"COMP: {summary['comp_value_counts']}")
    print("\nThong ke cac cot so:")
    with pd.option_context("display.width", 200, "display.max_columns", 30):
        print(df.describe().T[["mean", "std", "min", "50%", "max"]].round(3))

    leak_rows = []
    for start, end in REPORTED_AIR_LEAKS:
        mask = (df["timestamp"] >= start) & (df["timestamp"] <= end)
        leak_rows.append({"start": start, "end": end, "rows": int(mask.sum())})
    summary["reported_air_leaks"] = leak_rows
    print(f"\nCac doan ro khi duoc bao cao (so dong co du lieu): {leak_rows}")

    OUT.mkdir(parents=True, exist_ok=True)
    parquet_path = OUT / "metropt3.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"Da luu {parquet_path.relative_to(ROOT)} ({parquet_path.stat().st_size / 1e6:.1f} MB)")
    return summary


def eda_skab() -> dict:
    base = RAW / "SKAB" / "data"
    print(f"\n=== SKAB: {base.relative_to(ROOT)}")
    files = sorted(base.rglob("*.csv"))
    rows, anomalies, intervals = 0, 0, []
    per_group: dict[str, int] = {}
    columns = None
    for f in files:
        d = pd.read_csv(f, sep=";", parse_dates=["datetime"])
        columns = columns or list(d.columns)
        rows += len(d)
        if "anomaly" in d:
            anomalies += int(d["anomaly"].sum())
        intervals.append(d["datetime"].diff().dt.total_seconds().median())
        per_group[f.parent.name] = per_group.get(f.parent.name, 0) + 1
    summary = {
        "files": len(files),
        "files_per_group": per_group,
        "rows": rows,
        "anomaly_ratio": anomalies / rows if rows else 0.0,
        "median_dt_seconds": float(pd.Series(intervals).median()),
        "columns": columns,
    }
    print(f"So file: {summary['files']} {per_group}")
    print(f"Tong dong: {rows:,}, ty le bat thuong: {summary['anomaly_ratio']:.1%}")
    print(f"Khoang cach mau median (s): {summary['median_dt_seconds']}")
    print(f"Cot: {columns}")
    return summary


def main() -> None:
    result = {"metropt3": eda_metropt(), "skab": eda_skab()}
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "eda_summary.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nTom tat luu tai {(OUT / 'eda_summary.json').relative_to(ROOT)}")


if __name__ == "__main__":
    main()
