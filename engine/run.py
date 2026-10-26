"""Dong co streaming: doc features_live qua Change Streams, tinh HI, RUL, phat hien silent failure, cap nhat device_state."""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Dam bao UTF-8 an toan tren Windows console
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

from fp_algo.detect import Cusum
from fp_algo.predictor import BasePredictor, create_predictor
from mongo.client import (
    create_mongo_client,
    get_mongo_database_name,
    get_mongo_uri,
    sanitize_uri,
)

# Tai cau hinh .env
load_dotenv()

MONGO_URI = get_mongo_uri()
DB_NAME = get_mongo_database_name()
N_SHARDS = int(os.getenv("N_SHARDS") or "4")
PREDICTOR_MODE = os.getenv("PREDICTOR_MODE", "baseline")
AI_MODEL_PATH = os.getenv("AI_MODEL_PATH", "models/fleetpulse_model.joblib")

# Cac nguong cau hinh tap trung (co the override qua bien moi truong)
DEGRADING_HI_THRESHOLD = float(os.getenv("DEGRADING_HI_THRESHOLD", "4.8"))
RECOVERY_HI_THRESHOLD = float(os.getenv("RECOVERY_HI_THRESHOLD", "4.4"))
HI_FAILURE_THRESHOLD = float(os.getenv("HI_FAILURE_THRESHOLD", "6.0"))
SILENCE_TIMEOUT_SEC = float(os.getenv("SILENCE_TIMEOUT_SEC", "25.0"))
CUSUM_MU = float(os.getenv("CUSUM_MU", "4.1"))
CUSUM_SIGMA = float(os.getenv("CUSUM_SIGMA", "0.5"))
CUSUM_K = float(os.getenv("CUSUM_K", "0.5"))
CUSUM_H = float(os.getenv("CUSUM_H", "2.5"))


def parse_utc_datetime(val: Any) -> datetime:
    """Chuyen doi linh hoat timestamp sang UTC datetime an toan."""
    if isinstance(val, datetime):
        return val.astimezone(timezone.utc) if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, str) and val.strip():
        try:
            dt = datetime.fromisoformat(val.strip().replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return datetime.now(timezone.utc)


class DeviceProcessor:
    """Theo doi trang thai va chay thuat toan phan tich cho 1 thiet bi (Thread-safe)."""

    def __init__(
        self,
        device_id: str,
        predictor_mode: str = PREDICTOR_MODE,
        model_path: str | Path | None = AI_MODEL_PATH,
        baseline: dict[str, Any] | None = None,
        initial_cusum_pos: float = 0.0,
        degrading_hi_threshold: float = DEGRADING_HI_THRESHOLD,
        recovery_hi_threshold: float = RECOVERY_HI_THRESHOLD,
    ) -> None:
        self.device_id = device_id
        self.predictor_mode = predictor_mode
        self.degrading_hi_threshold = degrading_hi_threshold
        self.recovery_hi_threshold = recovery_hi_threshold
        self.lock = threading.Lock()

        # Khoi tao baseline tham chieu chuan cho MetroPT-3
        baseline_default = baseline or {
            "mean": [1.415, 3.28, 0.42, 0.07, 38.0],
            "inv_cov": [
                [0.1, 0.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 25.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 100.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.01],
            ],
        }

        # Khoi tao predictor chuan hoa (Rule-based hoac ML voi seamless fallback)
        self.predictor = create_predictor(
            mode=predictor_mode,
            model_path=model_path,
            baseline=baseline_default,
            hi_failure_threshold=HI_FAILURE_THRESHOLD,
        )
        self.cusum = Cusum(mu=CUSUM_MU, sigma=CUSUM_SIGMA, k=CUSUM_K, h=CUSUM_H, two_sided=False)
        if initial_cusum_pos > 0.0:
            self.cusum.pos = float(initial_cusum_pos)

        self.last_seen_wall = time.time()
        self.last_seen_dt = datetime.now(timezone.utc)
        self.last_window_start: str | None = None
        self.healthy_streak = 0

        self.status = "ok"
        self.hi = 1.0
        self.rul_hours = 120.0
        self.rul_low = 90.0
        self.rul_high = 150.0

    def process_features(self, feat_doc: dict[str, Any]) -> dict[str, Any]:
        """Xu ly mot cua so dac trung moi (Thread-safe & Idempotent theo windowStart)."""
        with self.lock:
            now_wall = time.time()
            now_utc = datetime.now(timezone.utc)
            self.last_seen_wall = now_wall

            w_start_str = str(feat_doc.get("windowStart") or "")
            if w_start_str:
                self.last_seen_dt = parse_utc_datetime(w_start_str)
            else:
                self.last_seen_dt = now_utc

            feat = feat_doc.get("feat")
            if not isinstance(feat, dict):
                feat = {}

            # Kiem tra idempotency: Neu cung mot cua so bi update lai, tranh tinh luy CUSUM lap lai
            is_new_window = (w_start_str != self.last_window_start)
            if is_new_window:
                self.last_window_start = w_start_str

            # 1. Tinh Health Index (HI)
            self.hi = float(self.predictor.predict_health(feat))

            # 2. Chay CUSUM tren HI (chi update khi la cua so moi)
            if is_new_window:
                is_cusum_alert = bool(self.cusum.update(self.hi))
            else:
                is_cusum_alert = bool(self.cusum.pos >= self.cusum.h)

            # 3. Cap nhat RUL qua Predictor
            rul_info = self.predictor.predict_rul(feat, self.hi, t=now_wall)
            self.rul_hours = float(rul_info["rul_hours"])
            self.rul_low = float(rul_info["rul_low"])
            self.rul_high = float(rul_info["rul_high"])

            # 4. Co che xac nhan phuc hoi (Recovery Confirmation):
            # Neu HI tro ve muc an toan (< recovery_hi_threshold) trong 2 cua so lien tiep -> giai toa CUSUM alert
            recovery_threshold = self.recovery_hi_threshold
            if self.hi < recovery_threshold:
                self.healthy_streak += 1
                if self.healthy_streak >= 2:
                    self.cusum.reset()
                    is_cusum_alert = False
            else:
                self.healthy_streak = 0

            # 5. Danh gia trang thai (HI >= 3.5 la bat dau suy giam, HI >= 6.0 la nguong hong)
            if is_cusum_alert or self.hi >= self.degrading_hi_threshold:
                self.status = "degrading"
            else:
                self.status = "ok"

            return {
                "deviceId": self.device_id,
                "status": self.status,
                "hi": round(self.hi, 3),
                "rulHours": round(self.rul_hours, 1),
                "rulHoursLow": round(self.rul_low, 1),
                "rulHoursHigh": round(self.rul_high, 1),
                "confidence": rul_info.get("confidence"),
                "predictorMode": self.predictor_mode,
                "edge": {
                    "cusum_pos": round(float(self.cusum.pos), 3),
                    "cusum_alert": is_cusum_alert,
                },
                "lastSeen": self.last_seen_dt,
                "engineTs": now_utc.isoformat(),
            }

    def process_silence(self) -> dict[str, Any] | None:
        """Xu ly khi thiet bi bi mat ket noi / im lang (Silent failure)."""
        with self.lock:
            now_wall = time.time()
            now_utc = datetime.now(timezone.utc)
            silence_duration = now_wall - self.last_seen_wall

            if silence_duration > SILENCE_TIMEOUT_SEC:
                self.status = "silent"
                rul_info = self.predictor.predict_silence(now_wall)
                self.rul_hours = float(rul_info["rul_hours"])
                self.rul_low = float(rul_info["rul_low"])
                self.rul_high = float(rul_info["rul_high"])

                # LUU Y CHUYEN MON: lastSeen la thoi diem thuc su nhan du lieu cuoi cung (last_seen_dt),
                # TUYET DOI KHONG gan now_utc lam sai lech thoi gian mat tin hieu!
                return {
                    "deviceId": self.device_id,
                    "status": self.status,
                    "hi": round(self.hi, 3),
                    "rulHours": round(self.rul_hours, 1),
                    "rulHoursLow": round(self.rul_low, 1),
                    "rulHoursHigh": round(self.rul_high, 1),
                    "confidence": rul_info.get("confidence"),
                    "predictorMode": self.predictor_mode,
                    "silenceDurationSec": round(silence_duration, 1),
                    "edge": {
                        "cusum_pos": round(float(self.cusum.pos), 3),
                    },
                    "lastSeen": self.last_seen_dt,
                    "engineTs": now_utc.isoformat(),
                }
            return None


class StreamingEngine:
    """Worker Engine doc Change Stream tu collection features_live voi co che tu phuc hoi."""

    def __init__(
        self,
        shard_id: int | str = "all",
        total_shards: int = N_SHARDS,
        mongo_uri: str = MONGO_URI,
        db_name: str = DB_NAME,
        predictor_mode: str = PREDICTOR_MODE,
        model_path: str | Path | None = AI_MODEL_PATH,
    ) -> None:
        if str(shard_id).lower() in ("all", "-1") or (isinstance(shard_id, int) and shard_id < 0):
            self.shard_id = "all"
        else:
            self.shard_id = int(shard_id)

        self.total_shards = total_shards
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.predictor_mode = predictor_mode
        self.model_path = model_path

        self.mongo_client = create_mongo_client(self.mongo_uri, timeout_ms=5000)
        self.db = self.mongo_client[self.db_name]
        self.features_col = self.db["features_live"]
        self.state_col = self.db["device_state"]
        self.incidents_col = self.db["incidents"]
        self.checkpoint_col = self.db["engine_checkpoints"]

        self._ensure_indexes()

        self.processors: dict[str, DeviceProcessor] = {}
        self.processors_lock = threading.Lock()
        self.running = True

        # Checkpoint resume token
        self.checkpoint_id = f"shard_{self.shard_id}"
        self.last_saved_token: Any = None

    def _ensure_indexes(self) -> None:
        """Dam bao index tren incidents va device_state."""
        try:
            self.state_col.create_index([("deviceId", 1)], unique=True, background=True)
            self.incidents_col.create_index([("deviceId", 1), ("status", 1)], background=True)
            self.checkpoint_col.create_index([("updatedAt", -1)], background=True)
        except Exception as exc:
            print(f"[WARN] Khong the tao index phu: {exc}")

    def _get_or_create_processor(self, device_id: str) -> DeviceProcessor:
        """Lay hoac tao DeviceProcessor co khoi phuc cusum_pos tu database."""
        with self.processors_lock:
            if device_id not in self.processors:
                # Thu khoi phuc trang thai CUSUM tu state truoc do de dam bao tinh lien tuc
                initial_cusum_pos = 0.0
                try:
                    old_state = self.state_col.find_one({"deviceId": device_id})
                    if old_state and "edge" in old_state:
                        initial_cusum_pos = float(old_state["edge"].get("cusum_pos", 0.0) or 0.0)
                except Exception:
                    pass

                self.processors[device_id] = DeviceProcessor(
                    device_id=device_id,
                    predictor_mode=self.predictor_mode,
                    model_path=self.model_path,
                    initial_cusum_pos=initial_cusum_pos,
                )
            return self.processors[device_id]

    def _sync_state(self, state_doc: dict[str, Any]) -> None:
        """Cap nhat device_state va quan ly tron ven vong doi incidents (mo va dong su co)."""
        d_id = state_doc["deviceId"]
        status = state_doc["status"]
        now_utc = datetime.now(timezone.utc)

        # 1. Cap nhat device_state
        self.state_col.update_one({"deviceId": d_id}, {"$set": state_doc}, upsert=True)

        # 2. Quan ly vong doi Incidents
        if status in ("degrading", "silent"):
            # Mo hoac cap nhat incident dang hoat dong
            severity = "critical" if status == "silent" else "high"
            evidence = (
                f"Trang thai {status} voi HI={state_doc['hi']}, RUL={state_doc['rulHours']}h "
                f"(Predictor: {state_doc.get('predictorMode')})"
            )
            incident_data = {
                "deviceId": d_id,
                "status": "active",
                "severity": severity,
                "hi": state_doc["hi"],
                "rulHours": state_doc["rulHours"],
                "evidence": evidence,
                "updatedAt": now_utc,
            }
            self.incidents_col.update_one(
                {"deviceId": d_id, "status": "active"},
                {
                    "$set": incident_data,
                    "$setOnInsert": {"openedAt": now_utc},
                },
                upsert=True,
            )
        elif status == "ok":
            # Khi thiet bi tro ve trang thai OK: Dong toan bo incident dang active cua thiet bi
            res = self.incidents_col.update_many(
                {"deviceId": d_id, "status": "active"},
                {
                    "$set": {
                        "status": "resolved",
                        "resolvedAt": now_utc,
                        "resolvedReason": "device_health_recovered_to_ok",
                        "final_hi": state_doc["hi"],
                        "final_rul": state_doc["rulHours"],
                        "updatedAt": now_utc,
                    }
                },
            )
            mod_count = getattr(res, "modified_count", 0)
            if isinstance(mod_count, int) and mod_count > 0:
                print(f"[*] Da dong thanh cong {mod_count} su co cho thiet bi {d_id} vi da phuc hoi ve OK.")

    def _silence_monitor_loop(self) -> None:
        """Thread kiem tra silent failure dinh ky (Thread-safe)."""
        while self.running:
            time.sleep(5.0)
            with self.processors_lock:
                procs = list(self.processors.values())

            for proc in procs:
                state_doc = proc.process_silence()
                if state_doc:
                    try:
                        self._sync_state(state_doc)
                    except Exception as exc:
                        print(f"[!] Loi cap nhat silent state cho {proc.device_id}: {exc}")

    def _save_checkpoint(self, resume_token: Any) -> None:
        """Luu resume_token vao collection engine_checkpoints de co the resume khi khoi dong lai."""
        if not resume_token:
            return
        try:
            self.checkpoint_col.update_one(
                {"_id": self.checkpoint_id},
                {
                    "$set": {
                        "resume_token": resume_token,
                        "updatedAt": datetime.now(timezone.utc),
                    }
                },
                upsert=True,
            )
            self.last_saved_token = resume_token
        except Exception as exc:
            print(f"[WARN] Khong the luu resume token checkpoint: {exc}")

    def _load_checkpoint(self) -> Any:
        """Doc resume_token gan nhat tu collection engine_checkpoints."""
        try:
            doc = self.checkpoint_col.find_one({"_id": self.checkpoint_id})
            if doc and "resume_token" in doc:
                return doc["resume_token"]
        except Exception as exc:
            print(f"[WARN] Khong the doc checkpoint cu: {exc}")
        return None

    def _build_pipeline(self) -> list[dict[str, Any]]:
        """Xay dung pipeline loc shard loai tru lan nhau, khong de worker tranh chap document."""
        if self.shard_id == "all":
            match_criteria = {"operationType": {"$in": ["insert", "replace", "update"]}}
        elif self.shard_id == 0:
            # Shard 0 dam nhiem ca nhung document thieu truong shard (neu co) de tranh bo sot
            match_criteria = {
                "operationType": {"$in": ["insert", "replace", "update"]},
                "$or": [
                    {"fullDocument.shard": 0},
                    {"fullDocument.shard": {"$exists": False}},
                ],
            }
        else:
            # Cac shard con lai CHI nhan dung shard cua minh, tuyet doi khong tranh chap
            match_criteria = {
                "operationType": {"$in": ["insert", "replace", "update"]},
                "fullDocument.shard": self.shard_id,
            }
        return [{"$match": match_criteria}]

    def run(self) -> None:
        shard_desc = "ALL SHARDS" if self.shard_id == "all" else f"Shard {self.shard_id}/{self.total_shards}"
        print(f"[*] Khoi dong Streaming Engine ({shard_desc}) [Predictor Mode: {self.predictor_mode}]...")
        print(f"[*] MongoDB Target: {sanitize_uri(self.mongo_uri)} -> DB: {self.db_name}")

        # Khoi dong thread giam sat silence failure
        t_silence = threading.Thread(target=self._silence_monitor_loop, daemon=True)
        t_silence.start()

        pipeline = self._build_pipeline()
        backoff_sec = 1.0

        # Vong lap tu phuc hoi (Self-healing Reconnect Loop)
        while self.running:
            resume_token = self._load_checkpoint()
            watch_kwargs: dict[str, Any] = {
                "pipeline": pipeline,
                "full_document": "updateLookup",
            }
            if resume_token:
                watch_kwargs["resume_after"] = resume_token

            try:
                print(f"[OK] Mo Change Stream tren features_live ({shard_desc}) [resume_token={'co' if resume_token else 'khong'}]...")
                with self.features_col.watch(**watch_kwargs) as stream:
                    backoff_sec = 1.0  # Reset backoff khi ket noi thanh cong
                    for change in stream:
                        if not self.running:
                            break

                        change_id = change.get("_id")
                        doc = change.get("fullDocument")
                        if not doc or not isinstance(doc, dict):
                            self._save_checkpoint(change_id)
                            continue

                        d_id = doc.get("deviceId")
                        if not d_id or not isinstance(d_id, str):
                            self._save_checkpoint(change_id)
                            continue

                        # Per-record isolation: loi cua 1 document khong lam sap toan bo worker stream
                        try:
                            proc = self._get_or_create_processor(d_id.strip())
                            state = proc.process_features(doc)
                            self._sync_state(state)
                        except Exception as doc_err:
                            print(f"[!] Loi xu ly features cho thiet bi {d_id}: {doc_err}", file=sys.stderr)

                        # Luu checkpoint resume token sau moi su co/su kien thanh cong
                        self._save_checkpoint(change_id)

            except OperationFailure as of:
                # Ma loi 280 (ChangeStreamHistoryLost) hoac 286 (ChangeStreamFatalError)
                if of.code in (280, 286) or "resume" in str(of).lower():
                    print(f"[WARN] Resume token da qua cu hoac khong hop le ({of}), mo stream moi tu hien tai...")
                    self._save_checkpoint(None)
                    time.sleep(1.0)
                else:
                    if not self.running:
                        break
                    print(f"[!] OperationFailure Change Stream: {of}. Thu lai sau {backoff_sec:.1f} giay...", file=sys.stderr)
                    time.sleep(backoff_sec)
                    backoff_sec = min(30.0, backoff_sec * 2.0)
            except PyMongoError as pme:
                if not self.running:
                    break
                print(f"[!] Mat ket noi Change Stream: {pme}. Thu lai sau {backoff_sec:.1f} giay...", file=sys.stderr)
                time.sleep(backoff_sec)
                backoff_sec = min(30.0, backoff_sec * 2.0)
            except KeyboardInterrupt:
                print("\n[*] Nhan tin hieu dung Engine theo yeu cau.")
                self.running = False
                break
            except Exception as e:
                if not self.running:
                    break
                print(f"[!] Exception ngoai y muon trong Change Stream loop: {e}", file=sys.stderr)
                time.sleep(2.0)

        self.running = False
        try:
            self.mongo_client.close()
        except Exception:
            pass
        print(f"[*] Engine ({shard_desc}) da dung hoan tat.")


def main() -> None:
    parser = argparse.ArgumentParser(description="FleetPulse Streaming Engine Worker")
    parser.add_argument("--shard", default="all", help="Shard ID (0 -> shards-1) hoac 'all'")
    parser.add_argument("--shards", type=int, default=N_SHARDS, help="Tong so shard")
    parser.add_argument("--mongo-uri", default=MONGO_URI, help="MongoDB connection URI")
    parser.add_argument("--db", default=DB_NAME, help="Database name")
    parser.add_argument("--predictor-mode", default=PREDICTOR_MODE, choices=["baseline", "ai", "ml"], help="Che do predictor")
    parser.add_argument("--model-path", default=AI_MODEL_PATH, help="Duong dan file mo hinh AI")
    args = parser.parse_args()

    engine = StreamingEngine(
        shard_id=args.shard,
        total_shards=args.shards,
        mongo_uri=args.mongo_uri,
        db_name=args.db,
        predictor_mode=args.predictor_mode,
        model_path=args.model_path,
    )
    engine.run()


if __name__ == "__main__":
    main()
