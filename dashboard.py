"""FleetPulse Real-time Interactive Monitoring Dashboard (Streamlit).

Chạy bằng:
    streamlit run dashboard.py --server.port 8501
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
from pymongo import MongoClient

from mongo.client import get_mongo_database_name, get_mongo_uri
from scheduler.app import run_greedy_scheduler
from simulator.inject import inject_fault

load_dotenv()

st.set_page_config(
    page_title="FleetPulse - Train APU Predictive Maintenance",
    page_icon="🚆",
    layout="wide",
    initial_sidebar_state="expanded",
)

MONGO_URI = get_mongo_uri()
DB_NAME = get_mongo_database_name()


@st.cache_resource
def get_mongo_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)


def get_db():
    client = get_mongo_client()
    return client[DB_NAME]


def format_local_time(ts_val) -> str:
    """Chuyển đổi ISO string sang giờ thực tế địa phương của máy tính."""
    if not ts_val:
        return ""
    try:
        if isinstance(ts_val, str):
            clean = ts_val.replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean)
        elif isinstance(ts_val, datetime):
            dt = ts_val
        else:
            return str(ts_val)[:19]
        if dt.tzinfo is not None:
            local_dt = dt.astimezone()
            return local_dt.strftime("%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts_val)[:19].replace("T", " ")


# CSS Tùy biến giao diện hiện đại
st.markdown(
    """
    <style>
    .metric-card {
        background-color: #1e293b;
        border-radius: 10px;
        padding: 15px;
        border: 1px solid #334155;
        color: white;
    }
    .status-ok {
        color: #22c55e;
        font-weight: bold;
    }
    .status-degrading {
        color: #f59e0b;
        font-weight: bold;
    }
    .status-silent {
        color: #ef4444;
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Header
st.title("🚆 FleetPulse: Nền Tảng IoT & AI Dự Báo Bảo Trì Đội Tàu")
st.caption("Giám sát sức khỏe máy nén khí (APU) thời gian thực • Ước lượng RUL • Lập lịch bảo trì kinh tế")

# Sidebar
with st.sidebar:
    st.header("⚙️ Cấu Hình & Điều Khiển")
    st.info(f"**Database:** `{DB_NAME}`\n\n**Mode:** `{os.getenv('PREDICTOR_MODE', 'ai').upper()}`")
    
    auto_refresh = st.checkbox("Tự động làm mới (Auto Refresh)", value=True)
    refresh_rate = st.slider("Tần số làm mới (giây)", min_value=2, max_value=15, value=3)

    st.divider()
    st.subheader("⚠️ Tiêm Lỗi Thử Nghiệm (Fault Injection)")
    dev_inject = st.selectbox("Chọn tàu tiêm lỗi:", [f"M-{i:03d}" for i in range(1, 21)], index=16) # M-017
    fault_type = st.selectbox("Loại sự cố:", ["air_leak", "sensor_drift", "stuck_comp"], index=0)
    severity = st.slider("Mức độ nghiêm trọng (Severity):", 0.1, 1.0, 0.7, 0.05)
    
    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("🚨 Tiêm Lỗi", use_container_width=True):
            try:
                res = inject_fault(dev_inject, fault_type, severity)
                st.success(f"Đã tiêm {fault_type} ({severity}) cho {dev_inject}!")
            except Exception as e:
                st.error(f"Lỗi: {e}")
    with col_btn2:
        if st.button("🔧 Sửa Chữa (Resolve)", use_container_width=True):
            try:
                db = get_db()
                db["fault_labels"].update_many(
                    {"deviceId": dev_inject, "resolved": {"$ne": True}},
                    {"$set": {"resolved": True, "resolvedAt": datetime.now(timezone.utc)}},
                )
                st.info(f"Đã giải tỏa lỗi cho {dev_inject}!")
            except Exception as e:
                st.error(f"Lỗi: {e}")

    st.divider()
    if st.button("📅 Kích Hoạt Scheduler (Lập Lịch)", use_container_width=True):
        try:
            res_sched = run_greedy_scheduler(capacity=3, horizon_hours=72.0)
            st.success(f"Đã tạo {res_sched['scheduled_count']} lệnh bảo trì mới!")
        except Exception as e:
            st.error(f"Lỗi Scheduler: {e}")

# Fetch Data
db = get_db()
try:
    devices = list(db["device_state"].find())
    incidents = list(db["incidents"].find().sort("openedAt", -1).limit(20))
    work_orders = list(db["work_orders"].find().sort("createdAt", -1).limit(20))
    telemetry_count = db["telemetry"].count_documents({})
    features_count = db["features_live"].count_documents({})
except Exception as e:
    st.error(f"Không thể truy vấn MongoDB: {e}")
    devices, incidents, work_orders, telemetry_count, features_count = [], [], [], 0, 0

# Status count
count_ok = sum(1 for d in devices if d.get("status") == "ok")
count_deg = sum(1 for d in devices if d.get("status") == "degrading")
count_silent = sum(1 for d in devices if d.get("status") == "silent")
active_incidents = [inc for inc in incidents if inc.get("status") == "active"]

# Top Metrics Row
m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Tổng Số Đoàn Tàu", len(devices) if devices else 20, delta="Đang chạy")
m2.metric("Trạng Thái Bình Thường (OK)", count_ok, delta=f"{count_ok}/20")
m3.metric("Đang Suy Thoái (Degrading)", count_deg, delta=f"-{count_deg}" if count_deg > 0 else "0", delta_color="inverse")
m4.metric("Sự Cố Đang Mở (Active)", len(active_incidents), delta="Cần xử lý" if active_incidents else "An toàn", delta_color="inverse")
m5.metric("Lệnh Bảo Trì (Work Orders)", len(work_orders), delta="Greedy Optimizer")

st.divider()

# Tabs
tab_fleet, tab_detail, tab_action = st.tabs(["📊 Tổng Quan Đội Tàu (Fleet Grid)", "🔍 Chi Tiết Đoàn Tàu (Deep Dive)", "📋 Sự Cố & Lệnh Bảo Trì (Incidents & Actions)"])

with tab_fleet:
    if devices:
        table_data = []
        for d in devices:
            status = d.get("status", "ok")
            status_icon = "🟢 OK" if status == "ok" else ("🟡 Suy Thoái" if status == "degrading" else "🔴 Lỗi Câm")
            table_data.append({
                "Mã Tàu": d.get("deviceId"),
                "Trạng Thái": status_icon,
                "Chỉ Số HI": round(float(d.get("hi", 1.0)), 2),
                "Tuổi Thọ Còn Lại (RUL)": f"{round(float(d.get('rulHours', 120.0)), 1)} giờ",
                "Khoảng Tin Cậy RUL": f"[{round(float(d.get('rulHoursLow', 90)), 1)} - {round(float(d.get('rulHoursHigh', 150)), 1)}] h",
                "CUSUM Score": d.get("edge", {}).get("cusum_pos", 0.0),
                "Giờ Nhận Tin Thực Tế": format_local_time(d.get("engineTs")),
                "Mốc Chu Kỳ MetroPT": str(d.get("lastSeen", ""))[:19],
            })
        df_display = pd.DataFrame(table_data)
        st.dataframe(df_display, use_container_width=True, height=450)
    else:
        st.info("Đang chờ Ingestion Service và Streaming Engine ghi nhận dữ liệu đội tàu... (Hãy chắc chắn runner đang chạy)")

with tab_detail:
    col_sel, col_stat = st.columns([1, 3])
    with col_sel:
        selected_dev_id = st.selectbox("Chọn tàu xem chi tiết:", [d.get("deviceId") for d in devices] if devices else ["M-017"])
    
    selected_dev = next((d for d in devices if d.get("deviceId") == selected_dev_id), None)
    if selected_dev:
        hi_val = float(selected_dev.get("hi", 1.0))
        rul_val = float(selected_dev.get("rulHours", 120.0))
        rul_low = float(selected_dev.get("rulHoursLow", 90.0))
        rul_high = float(selected_dev.get("rulHoursHigh", 150.0))
        status = selected_dev.get("status", "ok")

        with col_stat:
            c1, c2, c3 = st.columns(3)
            c1.metric("Chỉ Số Health Index (HI)", round(hi_val, 3), delta="Cảnh báo: >= 4.8")
            c2.metric("RUL Dự Báo (Giờ)", f"{round(rul_val, 1)} h", delta=f"Khoảng: [{round(rul_low, 1)} - {round(rul_high, 1)}] h")
            c3.metric("Trạng Thái Vận Hành", status.upper(), delta="Bình thường" if status == "ok" else "Cần can thiệp")

        # Gauge bar HI
        hi_norm = min(1.0, max(0.0, hi_val / 8.0))
        st.subheader("Mức Độ Suy Thoái Sức Khỏe (Health Index)")
        st.progress(hi_norm)
        st.caption(f"HI hiện tại: {hi_val:.2f} / 8.0 (Ngưỡng an toàn < 4.8 • Ngưỡng hỏng hóc = 6.0)")

        # Lấy lịch sử 10 cửa sổ gần nhất của tàu này từ features_live
        recent_feats = list(db["features_live"].find({"deviceId": selected_dev_id}).sort("windowStart", -1).limit(20))
        if recent_feats:
            chart_data = []
            for rf in reversed(recent_feats):
                feat = rf.get("feat", {})
                real_time_str = format_local_time(rf.get("ts_chain", {}).get("ingest_ts") or rf.get("createdAt"))
                chart_data.append({
                    "Thời gian": real_time_str[-8:] if len(real_time_str) >= 8 else str(rf.get("windowStart", ""))[-8:],
                    "Áp suất TP2 (bar)": feat.get("tp2_mean", 0.0),
                    "Tỷ lệ Tải COMP": feat.get("loaded_ratio", 0.0),
                    "Nhiệt độ dầu (°C)": feat.get("oil_temp_mean", 0.0),
                    "Dòng Motor (A)": feat.get("motor_current_mean", 0.0),
                })
            df_chart = pd.DataFrame(chart_data)
            st.subheader(f"Biểu Đồ Cảm Biến Cửa Sổ Gần Nhất ({selected_dev_id})")
            st.line_chart(df_chart.set_index("Thời gian"))
    else:
        st.warning(f"Chưa có dữ liệu chi tiết cho {selected_dev_id}.")

with tab_action:
    col_inc, col_wo = st.columns(2)
    with col_inc:
        st.subheader("🚨 Nhật Ký Sự Cố (Incidents)")
        if incidents:
            inc_list = []
            for inc in incidents:
                inc_list.append({
                    "Tàu": inc.get("deviceId"),
                    "Trạng Thái": "🔴 Active" if inc.get("status") == "active" else "🟢 Resolved",
                    "Mức Độ": str(inc.get("severity", "")).upper(),
                    "HI": inc.get("hi"),
                    "RUL": f"{inc.get('rulHours')} h",
                    "Thời Điểm": str(inc.get("openedAt", ""))[:19],
                })
            st.dataframe(pd.DataFrame(inc_list), use_container_width=True)
        else:
            st.info("Chưa ghi nhận sự cố nào.")

    with col_wo:
        st.subheader("🛠️ Lệnh Bảo Trì (Work Orders - Scheduler)")
        if work_orders:
            wo_list = []
            for wo in work_orders:
                wo_list.append({
                    "Ưu Tiên": f"P{wo.get('priority_rank', 1)}",
                    "Mã Tàu": wo.get("deviceId"),
                    "Trạng Thái": wo.get("status"),
                    "Ngày Dự Kiến": str(wo.get("scheduledDate", ""))[:10],
                    "Lý Do": wo.get("reason", "")[:50] + "...",
                })
            st.dataframe(pd.DataFrame(wo_list), use_container_width=True)
        else:
            st.info("Chưa có lệnh bảo trì nào. Nhấn 'Kích Hoạt Scheduler' ở thanh bên để tính toán.")

# Auto refresh logic
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()

