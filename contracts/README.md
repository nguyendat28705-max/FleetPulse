# FleetPulse Contracts & Data Schema

Tài liệu hợp đồng giao tiếp giữa các thành phần trong hệ thống FleetPulse theo mục 18.3 của báo cáo thiết kế.

## 1. MQTT Topics & Schemas

### 1.1 `fleet/{line}/{device}/telemetry`
Dữ liệu cảm biến thô từ Simulator hoặc Edge Gateway.
- Tham khảo mẫu: [telemetry.json](file:///f:/MUVNA/contracts/telemetry.json)
- Các trường quan trọng:
  - `deviceId`: Định danh máy (ví dụ: `M-017`)
  - `line`: Tuyến vận hành (`L1`, `L2`, ...)
  - `gatewayId`: Gateway phụ trách (`GW-01`, `GW-02`, ...)
  - `seq`: Số thứ tự tuần tự tăng dần theo thiết bị (phục vụ phát hiện mất/trùng/đảo thứ tự)
  - `ts`: Thời điểm ghi nhận mẫu cảm biến (ISO 8601 UTC)
  - `emit_ts`: Thời điểm phát bản tin (ISO 8601 UTC)
  - `values`: Map các giá trị cảm biến (`TP2`, `TP3`, `Motor_current`, `Oil_temperature`, `COMP`, ...)

### 1.2 `fleet/{line}/{device}/features`
Đặc trưng cửa sổ (mặc định 10s) do Gateway tính toán, được Ingestion đưa vào collection `features_live`.
- Tham khảo mẫu: [features.json](file:///f:/MUVNA/contracts/features.json)
- Khóa Unique trong MongoDB: `(deviceId, windowStart)`
- Các trường quan trọng:
  - `windowStart`: Bắt đầu cửa sổ (ISO 8601 UTC)
  - `windowSec`: Độ dài cửa sổ (mặc định 10s)
  - `n`: Số mẫu hợp lệ trong cửa sổ
  - `late`: Đánh dấu boolean nếu bản tin đến sau watermark
  - `feat`:
    - `tp2_mean`, `tp2_std`, `tp2_slope`: Áp suất TP2
    - `loaded_ratio`: Tỷ lệ thời gian COMP = 0 (đang tải)
    - `rise_rate`: Tốc độ tăng áp khi nén
    - `cycle_len_s`: Độ dài chu kỳ nén
  - `edge`:
    - `cusum_pos`: Giá trị CUSUM tại biên
    - `alert`: Trạng thái kích hoạt cảnh báo cục bộ
    - `mode`: `normal` hoặc `burst`
  - `shard`: `crc32(deviceId) % N_SHARDS` (do Ingestion gắn)
  - `ts_chain`: Chuỗi dấu thời gian `emit_ts`, `gateway_ts`, `ingest_ts` (và `engine_ts` khi engine xử lý) phục vụ đo độ trễ đầu cuối (U1).

### 1.3 `fleet/{line}/{device}/heartbeat`
Bản tin nhịp tim từ Gateway báo cáo trạng thái định kỳ.
- Tham khảo mẫu: [heartbeat.json](file:///f:/MUVNA/contracts/heartbeat.json)
- Các trường: `deviceId`, `line`, `gatewayId`, `ts`, `queueDepth`.

---

## 2. MongoDB Collections & Models

- `telemetry`: Time Series collection lưu dữ liệu thô (TTL 7 ngày). Không dùng Change Streams trên collection này.
- `features_live`: Collection thường, lưu đặc trưng 10s (TTL 1 giờ). **Là nguồn dữ liệu chính cho Change Streams**.
- `device_state`: Mỗi thiết bị 1 document duy nhất (`deviceId`, `status`, `hi`, `rulHours`, `rulHoursLow`, `hypothesisProbs`, `lastSeen`, `engineTs`).
- `incidents`: Sự cố phát hiện (`deviceId`, `openedAt`, `evidence`, `status`).
- `work_orders`: Lệnh bảo trì do Scheduler sinh ra.
- `fleet_rollup_1m`: Rollup 1 phút bằng `$merge`.
- `fault_labels`: Nhãn tiêm lỗi thực nghiệm (`deviceId`, `t0`, `severity`, `type`).
- `experiments`: Lưu kết quả benchmark & thử nghiệm.
