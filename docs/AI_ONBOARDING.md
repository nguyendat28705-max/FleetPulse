# HƯỚNG DẪN ONBOARDING AI / DATA SCIENCE CHO DỰ ÁN FLEETPULSE

Tài liệu dành cho Data Scientists, Machine Learning Engineers và Data Engineers tham gia phát triển mô hình AI chẩn đoán sức khỏe (Health Index) và dự báo tuổi thọ còn lại (RUL) cho hệ thống máy nén khí đoàn tàu MetroPT-3.

---

## 1. TỔNG QUAN TẬP DỮ LIỆU METROPT-3

Dataset **MetroPT-3** (UCI Machine Learning Repository #791) ghi nhận dữ liệu chuỗi thời gian đa biến từ Bộ sản xuất khí nén (Air Production Unit - APU) trên đoàn tàu điện ngầm tại Porto, Bồ Đào Nha từ **01/02/2020** đến **01/09/2020** (7 tháng liên tục).

### Thống kê tập dữ liệu gốc:
- **Tổng số bản ghi:** 1,516,948 dòng.
- **Tần số lấy mẫu:** ~10 giây / mẫu (0.1 Hz).
- **Trạng thái máy nén (`COMP`):**
  - `COMP = 0`: Máy nén đang chạy nạp khí (Under load) - chiếm ~16.3% thời gian.
  - `COMP = 1`: Máy nén không tải hoặc tắt (Idle/Off) - chiếm ~83.7% thời gian.
- **Áp suất TP2 vs TP3:**
  - `TP2` (Áp suất đầu ra máy nén): Dao động từ ~0 bar khi không tải lên 8.5 - 10.5 bar khi có tải (Trung bình: 1.368 bar, Độ lệch chuẩn: 3.251 bar).
  - `TP3` (Áp suất bình chứa điều khiển): Ổn định quanh mức làm việc 8.98 bar.

---

## 2. PIPELINE TIỀN XỬ LÝ VÀ ĐẶC TRƯNG HÓA

### 2.1. Quy trình tiền xử lý:
1. **Làm sạch:** Bỏ các cột không tên, chuẩn hóa kiểu timestamp UTC.
2. **Lưu trữ Parquet trung gian:** `data/processed/metropt3.parquet` (22 MB thay vì 218 MB CSV, nén Snappy).
3. **Cửa sổ hóa (Windowing):** Gom cụm 6 mẫu (~60 giây) không chồng lấn (`step = 6`) để tính đặc trưng thống kê và vật lý nén, đồng thời lọc bỏ các cửa sổ có khoảng trống mất dữ liệu (`gap > 60s`).

### 2.2. Danh sách 11 Đặc trưng chuẩn hóa (`fp_algo/contract.py`):

| Tên Đặc Trưng | Đơn vị | Ý nghĩa vật lý |
| :--- | :--- | :--- |
| `tp2_mean` | bar | Áp suất trung bình đầu ra máy nén |
| `tp2_std` | bar | Độ biến thiên áp suất TP2 trong cửa sổ |
| `tp2_slope` | bar/s | Độ dốc tăng/giảm áp suất theo hồi quy OLS |
| `loaded_ratio` | ratio | Tỷ lệ thời gian máy nén chạy nạp tải (`COMP == 0`) |
| `rise_rate` | bar/s | Tốc độ tăng áp trung bình khi nén tải |
| `cycle_len_s` | giây | Ước lượng độ dài chu kỳ nén giữa các lần bật tải |
| `oil_temp_mean` | °C | Nhiệt độ trung bình của dầu bôi trơn máy nén |
| `oil_temp_max` | °C | Nhiệt độ dầu bôi trơn cao nhất trong cửa sổ |
| `motor_current_mean` | A | Dòng điện động cơ trung bình |
| `motor_current_p95` | A | Phân vị 95% dòng điện động cơ |
| `delta_pressure` | bar | Chênh lệch áp suất trung bình: `TP2 - TP3` |

---

## 3. GÁN NHÃN GROUND TRUTH VÀ XỬ LÝ CENSERING

Các mốc sự cố rò khí được xác nhận trong thực tế (Davari et al., 2021):
- **Sự cố 1:** `2020-04-18 00:00:00` đến `2020-04-18 23:59:59`
- **Sự cố 2:** `2020-05-29 23:30:00` đến `2020-05-30 06:00:00`
- **Sự cố 3:** `2020-06-05 10:00:00` đến `2020-06-07 14:30:00`
- **Sự cố 4:** `2020-07-15 14:30:00` đến `2020-07-15 19:00:00`

### Quy tắc gán nhãn:
1. **`label_anomaly`:** Nhận giá trị `1` nếu cửa sổ nằm trong khoảng sự cố trên, nhận `0` nếu ở trạng thái hoạt động bình thường.
2. **`rul_hours` (Remaining Useful Life):**
   - Đếm ngược số giờ từ thời điểm kết thúc cửa sổ đến **thời điểm bắt đầu sự cố tiếp theo**.
   - Khi thiết bị đang trong sự cố: `rul_hours = 0.0`.
   - **Xử lý Right-Censoring:** Với các mẫu sau sự cố cuối cùng (sau ngày 15/07/2020) mà không ghi nhận thêm sự cố nào nữa, **không được gán `rul_hours = 0`** (vì thiết bị hoàn toàn khỏe mạnh). Cột `rul_hours` được để `NaN` và gắn cờ `rul_valid = False` (`event_type = 'right_censored'`).

---

## 4. CHIẾN LƯỢC CHIA TẬP DỮ LIỆU TIME-BASED (CHỐNG LEAKAGE)

Tuyệt đối **không dùng Random Split / K-Fold ngẫu nhiên** trên chuỗi thời gian vì sẽ gây rò rỉ dữ liệu tương lai (Lookahead Leakage).

Dataset được phân chia tuần tự theo trục thời gian tuyệt đối:

| Tập dữ liệu | File Parquet | Khoảng thời gian | Số mẫu | Tỷ lệ bất thường | Sự cố bao phủ |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Train** | `data/ai/train_features.parquet` | 01/02/2020 -> 31/05/2020 | 142,660 | 1.29% | Sự cố 1 & Sự cố 2 |
| **Validation**| `data/ai/val_features.parquet` | 01/06/2020 -> 30/06/2020 | 36,053 | 8.00% | Sự cố 3 (nghiêm trọng) |
| **Test** | `data/ai/test_features.parquet` | 01/07/2020 -> 01/09/2020 | 73,844 | 0.36% | Sự cố 4 + Hậu sửa chữa |

---

## 5. CÁCH HUẤN LUYỆN VÀ ĐÓNG GÓI MODEL CHO STREAMING

### 5.1. Ví dụ huấn luyện mô hình Scikit-Learn:
```python
import joblib
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from fp_algo.contract import FEATURE_ORDER

# 1. Đọc dữ liệu huấn luyện
train_df = pd.read_parquet("data/ai/train_features.parquet")
X_train = train_df[FEATURE_ORDER]
y_hi = train_df["label_anomaly"] * 8.0  # Ví dụ nhãn HI

# Lọc các mẫu RUL hợp lệ (loại bỏ censored)
rul_mask = train_df["rul_valid"] == True
X_rul_train = train_df.loc[rul_mask, FEATURE_ORDER]
y_rul_train = train_df.loc[rul_mask, "rul_hours"]

# 2. Huấn luyện Regressor
hi_model = HistGradientBoostingRegressor().fit(X_train, y_hi)
rul_model = HistGradientBoostingRegressor().fit(X_rul_train, y_rul_train)

# 3. Đóng gói Artifact theo chuẩn FleetPulse
bundle = {
    "hi_model": hi_model,
    "rul_model": rul_model,
    "feature_order": FEATURE_ORDER,
    "contract_version": "1.1.0",
}
joblib.dump(bundle, "models/fleetpulse_model.joblib")
```

### 5.2. Kích hoạt mô hình trong Streaming Engine:
Cấu hình trong file `.env`:
```env
PREDICTOR_MODE=ai
AI_MODEL_PATH=models/fleetpulse_model.joblib
```
Khi khởi động, `StreamingEngine` sẽ tự động tải mô hình, kiểm tra thứ tự 11 đặc trưng và thực hiện suy luận thời gian thực từ Change Streams. Nếu file mô hình bị lỗi hoặc thiếu, hệ thống tự động fallback về Rule-Based Baseline mà không ngắt luồng streaming.
