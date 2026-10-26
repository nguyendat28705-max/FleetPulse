# HƯỚNG DẪN THIẾT LẬP MONGODB CLOUD (ATLAS) & DI CHUYỂN DỮ LIỆU

Tài liệu hướng dẫn triển khai MongoDB Cloud (Atlas) cho dự án FleetPulse, cấu hình bảo mật, phân quyền người dùng, di chuyển dữ liệu (Migration), kiểm thử Change Streams và phương án Rollback.

---

## 1. YÊU CẦU KIẾN TRÚC MONGODB CLOUD

FleetPulse sử dụng hai tính năng đặc biệt của MongoDB:
1. **Time Series Collections (`telemetry`):** Tối ưu hóa dung lượng lưu trữ cảm biến IoT tần số cao (1 Hz).
2. **Change Streams (`features_live`):** Nền tảng cho Streaming Engine phản ứng theo thời gian thực (Event-Driven).

> [!IMPORTANT]
> **Yêu cầu Replica Set:** MongoDB Change Streams **bắt buộc** phải chạy trên Replica Set. MongoDB Atlas mặc định chạy ở chế độ 3-node Replica Set (ngay cả gói miễn phí M0 Sandbox), đáp ứng hoàn toàn yêu cầu này.
>
> **Lưu ý về Time Series:** MongoDB Time Series Collections không hỗ trợ Change Streams. Do đó, kiến trúc FleetPulse gom đặc trưng cửa sổ vào collection tiêu chuẩn `features_live` để Streaming Engine theo dõi qua Change Stream.

---

## 2. CÁC BƯỚC THIẾT LẬP MONGODB ATLAS

### Bước 1: Tạo Cluster
1. Đăng nhập [MongoDB Atlas](https://cloud.mongodb.com/).
2. Chọn **Build a Database** -> chọn tier **M0 (Free)** cho môi trường Dev hoặc **M10+** cho Production.
3. Chọn Cloud Provider và Region gần nhất (ví dụ: `AWS / ap-southeast-1 Singapore`).
4. Đặt tên Cluster: `fleetpulse-cluster`.

### Bước 2: Tạo Database User (Quyền tối thiểu)
1. Vào tab **Security** -> **Database Access** -> **Add New Database User**.
2. Chọn phương thức xác thực: **Password**.
3. Phân quyền (Built-in Role):
   - Chọn **Read and write to any database** (hoặc gán cụ thể `readWrite` trên database `fleetpulse_dev`).
   - Đối với Change Streams, người dùng cần có quyền đọc oplog (`read` trên admin/config database, mặc định user Atlas đã có quyền này cho cluster của mình).
4. Lưu mật khẩu an toàn.

### Bước 3: Cấu hình Network Access (IP Allowlist)
1. Vào tab **Security** -> **Network Access** -> **Add IP Address**.
2. **Thêm IP của máy chạy FleetPulse:**
   - IP hiện tại của bạn: `118.69.13.115/32`
   - Hoặc chọn **Add Current IP Address**.
   - (Tùy chọn Dev nhanh: `0.0.0.0/0` - Cho phép truy cập từ mọi nơi).
3. Đợi trạng thái chuyển sang **Active** (khoảng 1 phút).

> [!WARNING]
> Nếu chưa thêm IP vào Network Access, kết nối sẽ bị từ chối ở bước bắt tay TLS với lỗi:
> `[SSL: TLSV1_ALERT_INTERNAL_ERROR] tlsv1 alert internal error`

### Bước 4: Lấy Connection String
1. Vào tab **Database** -> nhấn **Connect** tại Cluster.
2. Chọn **Drivers** (Python 3.12+).
3. Sao chép chuỗi kết nối dạng:
   ```
   mongodb+srv://<username>:<password>@fleetpulse-cluster.hhmodr9.mongodb.net/?retryWrites=true&w=majority
   ```

---

## 3. CẤU HÌNH BIẾN MÔI TRƯỜNG (`.env`)

Cập nhật file `.env` tại thư mục gốc:

```env
# Chế độ: 'local' (Docker) hoặc 'cloud' (Atlas)
MONGODB_MODE=cloud

# Kết nối Cloud Atlas
MONGODB_URI=mongodb+srv://nguyendat28705_db_user:<MAT_KHAU_THAT>@fleetpulse-cluster.hhmodr9.mongodb.net/?retryWrites=true&w=majority
MONGODB_DATABASE=fleetpulse_dev

# Kết nối Local dự phòng
MONGODB_LOCAL_URI=mongodb://localhost:27017/?replicaSet=rs0&directConnection=true

# Cổng dịch vụ host
MONGO_HOST_PORT=27017
MQTT_HOST_PORT=1883
```

---

## 4. QUY TRÌNH DI CHUYỂN DỮ LIỆU (MIGRATION)

FleetPulse cung cấp công cụ `scripts/migrate_mongo.py` hỗ trợ di chuyển dữ liệu hai chiều có kiểm soát.

### 4.1. Chạy thử nghiệm (Dry-Run)
Kiểm tra số lượng bản ghi mà không ghi đè:
```powershell
.\.venv\Scripts\python.exe scripts/migrate_mongo.py --dry-run
```

### 4.2. Thực hiện di chuyển Local -> Cloud
Sao chép dữ liệu và tạo cấu trúc collection/index trên Cloud:
```powershell
.\.venv\Scripts\python.exe scripts/migrate_mongo.py
```
*Đặc tính của script:*
- Sử dụng `ReplaceOne(upsert=True)` theo `_id`: Hoàn toàn **Idempotent**, chạy lại nhiều lần không tạo bản ghi trùng.
- Tự động tạo các TTL Index (1 giờ cho `features_live`, 7 ngày cho `telemetry`, 30 ngày cho `fleet_rollup_1m`).
- Đối soát số lượng document nguồn và đích sau khi hoàn thành.
- Che mật khẩu trong logs.

### 4.3. Phương án Rollback về Local
Nếu môi trường Cloud gặp sự cố mạng hoặc chi phí, có thể rollback dữ liệu về Local:
```powershell
.\.venv\Scripts\python.exe scripts/migrate_mongo.py --reverse
```
Và đổi lại `MONGODB_MODE=local` trong `.env`.

---

## 5. KIỂM THỬ KẾT NỐI VÀ CHANGE STREAMS

Sau khi cấu hình IP trong Atlas, chạy bộ kiểm thử:
```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_mongo_cloud.py -v
```

Kết quả mong đợi:
- `test_local_mongodb_ping_and_replicaset`: PASSED
- `test_local_change_streams_event`: PASSED
- `test_cloud_mongodb_connectivity_or_blocked`: PASSED (nếu IP đã được mở) hoặc SKIPPED có hướng dẫn (nếu IP chưa mở).
