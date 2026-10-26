# FleetPulse: Nền Tảng IoT & AI Dự Báo Bảo Trì Đội Tàu (Predictive Maintenance Platform)

FleetPulse là hệ thống phân tích dữ liệu IoT và trí tuệ nhân tạo (AI/ML) thời gian thực phục vụ bảo trì dự đoán cho đội máy nén khí đoàn tàu (Air Production Unit - APU). Dự án tích hợp hoàn chỉnh hai luồng dữ liệu: **Historical Batch Pipeline** (chuẩn bị dữ liệu huấn luyện AI chuẩn time-series) và **Real-Time Streaming Pipeline** (phát hiện suy giảm sức khỏe, ước lượng tuổi thọ còn lại RUL và phát hiện lỗi câm theo thời gian thực).

---

## 1. TỔNG QUAN KIẾN TRÚC

```
[MetroPT-3 Dataset (UCI 791)] 
       │
       ▼
 [scripts/eda_datasets.py] ──> [data/processed/metropt3.parquet]
       │
       ▼
 [scripts/build_training_dataset.py] ──> [data/ai/*_features.parquet] (Train/Val/Test)
       │
       ▼ (Shared Feature Contract & Logic: fp_algo/features.py)
 [Virtual Simulator (20 tàu)] ──> [Mosquitto MQTT: 1883]
                                         │
                                         ▼
                               [Edge Gateway (10s Window)]
                                         │
                                         ▼
                               [Ingestion Service (CRC32 Sharding)]
                                         │
                                         ▼
                               [MongoDB Local / Atlas Cloud]
                                         │ (Change Streams trên features_live)
                                         ▼
                             [Streaming Engine Worker]
                                         │
                         ┌───────────────┴───────────────┐
                         ▼                               ▼
               [RuleBasedPredictor]             [MLPredictor (AI)]
               (Mahalanobis + Kalman)          (Scikit-Learn / Joblib)
                         │                               │
                         └───────────────┬───────────────┘
                                         ▼
                               [device_state & incidents]
                                         │
                                         ▼
                        [Scheduler & Action Service (FastAPI: 8002)]
                                         │
                                         ▼
                                   [work_orders]
```

---

## 2. YÊU CẦU HỆ THỐNG & CÀI ĐẶT

### 2.1. Yêu cầu tiên quyết:
- **Hệ điều hành:** Windows 10/11 (hỗ trợ PowerShell).
- **Python:** 3.12+ (đã tích hợp trong `.venv`).
- **Docker Desktop:** Đang chạy (để khởi tạo MongoDB 7.0 Replica Set và Mosquitto Broker).

### 2.2. Cài đặt môi trường:
```powershell
# Kích hoạt môi trường ảo
.\.venv\Scripts\Activate.ps1

# Cài đặt toàn bộ dependencies chuẩn hóa
pip install -r requirements.txt
```

---

## 3. CẤU HÌNH HỆ THỐNG (`.env`)

Sao chép file mẫu:
```powershell
Copy-Item .env.example .env
```

Nội dung cấu hình chính:
```env
# Chế độ MongoDB: 'local' (Docker) hoặc 'cloud' (Atlas)
MONGODB_MODE=local
MONGODB_URI=mongodb+srv://<user>:<password>@fleetpulse-cluster.hhmodr9.mongodb.net/?retryWrites=true&w=majority
MONGODB_DATABASE=fleetpulse_dev

# Chế độ Predictor: 'baseline' (Luật + Kalman) hoặc 'ai' (Mô hình học máy)
PREDICTOR_MODE=baseline
AI_MODEL_PATH=models/fleetpulse_model.joblib

# Cổng dịch vụ host
MONGO_HOST_PORT=27017
MQTT_HOST_PORT=1883
N_SHARDS=4
```

---

## 4. KHỞI CHẠY HỆ THỐNG (DEMO END-TO-END)

### 4.1. Khởi động nhanh toàn bộ hệ thống:
Chạy script điều phối tự động:
```powershell
.\scripts\demo.ps1 -Devices 20 -Speed 60
```
Script sẽ tự động:
1. Khởi động Docker container MongoDB (Replica Set `rs0`) và Mosquitto MQTT.
2. Khởi tạo các collection, TTL index và Time Series schema (`mongo/setup.js`).
3. Khởi chạy **Ingestion Service**, **Edge Gateway** (`--gateway-id all`), **Streaming Engine** (`--shard all`) và **Scheduler Service** (port 8002).
4. Khởi chạy **Fleet Simulator** phát luồng dữ liệu 20 đoàn tàu lên MQTT Broker.

### 4.2. Thử nghiệm Tiêm Lỗi (Fault Injection):
Mở một cửa sổ PowerShell mới để tiêm lỗi rò khí vào đoàn tàu `M-017`:
```powershell
.\.venv\Scripts\python.exe -m simulator.inject --device M-017 --type air_leak --severity 0.7
```
- Ngay sau khi tiêm lỗi, Gateway sẽ bắt độ lệch CUSUM, Ingestion ghi đặc trưng vào MongoDB, Streaming Engine phát hiện suy giảm chỉ số sức khỏe (`HI >= 3.5`), cập nhật trạng thái đoàn tàu sang `degrading`, giảm giờ RUL và tự động mở một bản ghi trong collection `incidents`.

### 4.3. Kiểm tra Trạng Thái & Lệnh Bảo Trì:
```powershell
# Xem danh sách lệnh bảo trì do Scheduler sinh ra:
Invoke-RestMethod http://localhost:8002/work_orders

# Kiểm tra trạng thái thiết bị trực tiếp trong MongoDB:
docker compose exec mongo mongosh --quiet "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true" --eval "use fleetpulse_dev; db.device_state.find({deviceId: 'M-017'}).pretty()"
```

### 4.4. Dừng toàn bộ hệ thống:
```powershell
.\scripts\down.ps1
```

---

## 5. MIGRATION LÊN MONGODB CLOUD (ATLAS)

Hệ thống hỗ trợ chuyển đổi linh hoạt giữa MongoDB Docker cục bộ và MongoDB Cloud (Atlas) thông qua biến môi trường `MONGODB_MODE`.

1. **Thiết lập IP Network Access:** Thêm IP hiện tại của bạn vào Atlas UI (xem hướng dẫn chi tiết tại [`docs/MONGODB_CLOUD_SETUP.md`](docs/MONGODB_CLOUD_SETUP.md)).
2. **Kiểm tra kết nối Cloud / Local:**
   ```powershell
   .\.venv\Scripts\python.exe scripts/check_status.py
   ```
3. **Di chuyển dữ liệu (Migration):**
   ```powershell
   # Dry-run kiểm tra:
   .\.venv\Scripts\python.exe scripts/migrate_mongo.py --dry-run

   # Thực hiện chuyển dữ liệu Local -> Cloud:
   .\.venv\Scripts\python.exe scripts/migrate_mongo.py
   ```
4. **Phương án Rollback:** Nếu cần quay về Local, chạy:
   ```powershell
   .\.venv\Scripts\python.exe scripts/migrate_mongo.py --reverse
   ```

---

## 6. PIPELINE DỮ LIỆU LỊCH SỬ CHO AI

Tạo các tập dữ liệu Parquet đã làm sạch, gán nhãn Ground Truth và chia Train / Validation / Test chuẩn chuỗi thời gian:

```powershell
# 1. Tải và tiền xử lý MetroPT-3 (tạo data/processed/metropt3.parquet)
.\.venv\Scripts\python.exe scripts/eda_datasets.py

# 2. Trích xuất đặc trưng và chia tập dữ liệu
.\.venv\Scripts\python.exe scripts/build_training_dataset.py
```

Kết quả đầu ra tại `data/ai/`:
- `train_features.parquet`: 142,660 dòng (Tháng 2 -> Tháng 5/2020) - 2 sự cố.
- `val_features.parquet`: 36,053 dòng (Tháng 6/2020) - Sự cố lớn ngày 05/06.
- `test_features.parquet`: 73,844 dòng (Tháng 7 -> Tháng 8/2020) - Sự cố ngày 15/07 + Hậu sửa chữa.
- `dataset_metadata.json`: Thống kê tỷ lệ nhãn, censoring và phạm vi thời gian.

Xem chi tiết hướng dẫn huấn luyện và định dạng model tại [`docs/AI_ONBOARDING.md`](docs/AI_ONBOARDING.md).

---

## 7. CHẠY KIỂM THỬ HỆ THỐNG

Chạy toàn bộ 21 kiểm thử:
```powershell
.\.venv\Scripts\python.exe -m pytest -v
```
Danh sách nhóm kiểm thử chi tiết xem tại [`docs/TESTING.md`](docs/TESTING.md).

---

## 8. DANH MỤC TÀI LIỆU KỸ THUẬT

- [Tài liệu Onboarding AI & Huấn luyện Model](docs/AI_ONBOARDING.md)
- [Hướng dẫn MongoDB Cloud & Migration](docs/MONGODB_CLOUD_SETUP.md)
- [Sơ đồ Kiến trúc Pipeline Dữ liệu (Batch & Streaming)](docs/DATA_PIPELINE.md)
- [Hướng dẫn và Báo cáo Kiểm thử](docs/TESTING.md)
- [Data Contract & Schema Đặc trưng](contracts/features.json)
