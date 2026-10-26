# HƯỚNG DẪN KIỂM THỬ HỆ THỐNG FLEETPULSE

Tài liệu hướng dẫn chạy bộ kiểm thử toàn diện cho FleetPulse, phân loại các nhóm kiểm thử và giải thích các trường hợp biên.

---

## 1. CÁCH CHẠY KIỂM THỬ

Mọi lệnh chạy từ thư mục gốc của dự án trên Windows PowerShell sử dụng môi trường ảo `.venv`:

```powershell
# Chạy toàn bộ test suite
.\.venv\Scripts\python.exe -m pytest -v

# Chạy có đo thời gian và hiển thị chi tiết print
.\.venv\Scripts\python.exe -m pytest -v -s

# Chạy một file test cụ thể
.\.venv\Scripts\python.exe -m pytest tests/test_feature_parity.py -v
```

---

## 2. PHÂN LOẠI CÁC NHÓM TEST SUITE

| Nhóm Kiểm Thử | File Test | Mục đích & Phạm vi | Số Test |
| :--- | :--- | :--- | :--- |
| **Thuật toán lõi (Core Algo)** | `tests/test_fp_algo.py` | Kiểm tra công thức trích xuất đặc trưng, CUSUM phát hiện trôi, thuật toán SDT nén mẫu, khoảng cách Mahalanobis HI và bộ lọc Kalman RUL. | 5 |
| **Predictor Interface & AI** | `tests/test_predictor.py` | Kiểm tra `BasePredictor`, `RuleBasedPredictor`, `MLPredictor` (nạp model scikit-learn thật), và cơ chế tự động Fallback về Baseline khi thiếu file model. | 4 |
| **Tính tương đương (Parity)** | `tests/test_feature_parity.py` | Đảm bảo tính toán online tại Edge Gateway và offline tại Batch Script cho kết quả đồng nhất ($\le 10^{-5}$), xử lý an toàn cửa sổ rỗng hoặc thiếu trường. | 2 |
| **Streaming Engine Logic** | `tests/test_engine.py` | Kiểm tra `DeviceProcessor` chuyển trạng thái từ `ok` sang `degrading` khi có rò khí, và phát hiện lỗi câm (`silent failure`) khi mất tín hiệu quá 25s. | 2 |
| **Dataset Pipeline & Leakage** | `tests/test_dataset_pipeline.py`| Kiểm tra các file Parquet (`train`, `val`, `test`), cấu trúc contract 11 features, tính hợp lệ của RUL và **chứng minh không có rò rỉ thời gian (No Temporal Leakage)**. | 3 |
| **MongoDB & Change Streams**| `tests/test_mongo_cloud.py` | Kiểm tra kết nối MongoDB Local Replica Set `rs0`, lắng nghe Change Streams thực tế trên `features_live`, và chẩn đoán kết nối Cloud Atlas. | 3 |
| **Tích hợp End-to-End (E2E)** | `tests/test_e2e_pipeline.py` | Giả lập chu trình hoàn chỉnh: Telemetry -> Gateway Features -> Ingestion Sharding -> DeviceProcessor -> Cập nhật State. | 1 |
| **Lập lịch Bảo trì (Scheduler)** | `tests/test_scheduler.py` | Kiểm tra thuật toán Greedy tối ưu chi phí dừng tàu vs chi phí sửa chữa chủ động. | 1 |

---

## 3. XỬ LÝ TRẠNG THÁI KIỂM THỬ ĐẶC BIỆT

### 3.1. Test MongoDB Cloud bị `SKIPPED`
Trong file `tests/test_mongo_cloud.py`:
- Test `test_cloud_mongodb_connectivity_or_blocked` sẽ báo trạng thái `SKIPPED` kèm thông báo:
  `[BLOCKED] MongoDB Cloud chưa cho phép IP hiện tại truy cập. Vui lòng thêm IP vào Atlas Network Access.`
- **Nguyên nhân:** Địa chỉ IP công cộng của bạn chưa được cấp phép trong MongoDB Atlas Project Network Access.
- **Cách khắc phục:** Truy cập Atlas UI -> **Network Access** -> **Add IP Address** -> Thêm IP của bạn (xem hướng dẫn tại `docs/MONGODB_CLOUD_SETUP.md`). Sau khi IP được duyệt, chạy lại test này sẽ chuyển sang `PASSED`.

### 3.2. Chạy kiểm thử khi chưa bật Docker
Nếu chưa khởi động Docker:
- Các test yêu cầu MongoDB cục bộ (`test_mongo_cloud.py`) sẽ thông báo không kết nối được cổng 27017.
- Khởi động hạ tầng trước khi test:
  ```powershell
  docker compose up -d --wait
  ```
