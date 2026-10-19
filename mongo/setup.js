// FleetPulse - tạo collection và index cho MongoDB.
// Chạy lại nhiều lần vẫn an toàn (idempotent).
//
// Cách chạy (PowerShell, từ thư mục gốc repo; thư mục mongo/ được mount vào /scripts):
//   docker compose exec mongo mongosh --quiet "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true" /scripts/setup.js
// Đổi tên database: thêm  -e MONGODB_DATABASE=ten_db  ngay sau "exec".
// (Thông báo in ra để không dấu cho khỏi lỗi font trên console Windows.)

const dbName = (typeof process !== "undefined" && process.env.MONGODB_DATABASE) || "fleetpulse_dev";
const fp = db.getSiblingDB(dbName);

function ensureCollection(name, options) {
  if (fp.getCollectionNames().includes(name)) {
    print(`= ${name} (da co)`);
    return;
  }
  fp.createCollection(name, options || {});
  print(`+ ${name} (tao moi)`);
}

// Dữ liệu cảm biến thô: Time Series, giữ 7 ngày.
// Lưu ý: Time Series KHÔNG hỗ trợ Change Streams -> engine đọc features_live.
ensureCollection("telemetry", {
  timeseries: { timeField: "ts", metaField: "meta", granularity: "seconds" },
  expireAfterSeconds: 60 * 60 * 24 * 7,
});
fp.telemetry.createIndex({ "meta.deviceId": 1, ts: -1 });

// Đặc trưng cửa sổ do gateway tính: nguồn sự kiện cho Change Streams, giữ 1 giờ.
ensureCollection("features_live");
fp.features_live.createIndex({ deviceId: 1, windowStart: 1 }, { unique: true });
fp.features_live.createIndex({ createdAt: 1 }, { expireAfterSeconds: 3600 });
fp.features_live.createIndex({ shard: 1 });

// Trạng thái hiện tại: 1 tài liệu cho mỗi thiết bị.
ensureCollection("device_state");
fp.device_state.createIndex({ deviceId: 1 }, { unique: true });
fp.device_state.createIndex({ status: 1, rulHoursLow: 1 });
fp.device_state.createIndex({ location: "2dsphere" });

// Sự cố và lệnh bảo trì.
ensureCollection("incidents");
fp.incidents.createIndex({ deviceId: 1, openedAt: -1 });
fp.incidents.createIndex({ status: 1 });

ensureCollection("work_orders");
fp.work_orders.createIndex({ deviceId: 1, createdAt: -1 });
fp.work_orders.createIndex({ status: 1, scheduledDate: 1 });

// Tổng hợp theo phút bằng $merge, giữ 30 ngày.
ensureCollection("fleet_rollup_1m");
fp.fleet_rollup_1m.createIndex({ minute: 1 }, { expireAfterSeconds: 60 * 60 * 24 * 30 });
fp.fleet_rollup_1m.createIndex({ deviceId: 1, minute: -1 });

// Nhãn lỗi tiêm (thiết bị, thời điểm t0, mức độ) để đo độ trễ phát hiện.
ensureCollection("fault_labels");
fp.fault_labels.createIndex({ deviceId: 1, t0: 1 });

// Kết quả benchmark / thực nghiệm.
ensureCollection("experiments");
fp.experiments.createIndex({ suite: 1, createdAt: -1 });

const userCollections = fp.getCollectionNames().filter((n) => !n.startsWith("system."));
print(`Xong: database "${dbName}" co ${userCollections.length} collection: ${userCollections.sort().join(", ")}`);
