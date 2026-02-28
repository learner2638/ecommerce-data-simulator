from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from pathlib import Path
from datetime import datetime
import uuid
import threading

from service import run_once

# ========================
# App
# ========================
app = FastAPI()

# ========================
# CORS（同域后其实不需要，但保留无伤）
# ========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 开发期全放开
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========================
# 静态网页挂载：/ui
# ========================
BASE_DIR = Path(__file__).resolve().parent        # .../web
STATIC_DIR = BASE_DIR / "static"                 # .../web/static

app.mount("/ui", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")


@app.get("/")
def home():
    return RedirectResponse(url="/ui/")


# ========================
# 任务存储
# ========================
TASKS = {}

# ✅ 最低成本护栏：防误操作写爆机器
MAX_ORDER_CNT = 200_000
MAX_SKU_CNT = 200_000

# ✅ 可选：限制同时运行/排队的任务数（不想要就改大点或删掉）
MAX_ACTIVE_JOBS = 3


class JobReq(BaseModel):
    mode: str = "dev"
    overrides: dict = {}
    do_export: bool = True


def _apply_guardrails(req: JobReq) -> None:
    """限制生成规模，避免 CPU/磁盘被误操作打爆。"""
    oc = req.overrides.get("order_cnt", None)
    sc = req.overrides.get("sku_cnt", None)

    if oc is not None:
        try:
            oc = int(oc)
        except Exception:
            raise HTTPException(400, "order_cnt must be int")
        if oc <= 0:
            raise HTTPException(400, "order_cnt must be > 0")
        if oc > MAX_ORDER_CNT:
            raise HTTPException(400, f"order_cnt too large (max {MAX_ORDER_CNT})")

    if sc is not None:
        try:
            sc = int(sc)
        except Exception:
            raise HTTPException(400, "sku_cnt must be int")
        if sc <= 0:
            raise HTTPException(400, "sku_cnt must be > 0")
        if sc > MAX_SKU_CNT:
            raise HTTPException(400, f"sku_cnt too large (max {MAX_SKU_CNT})")


def _active_jobs_count() -> int:
    return sum(
        1 for t in TASKS.values()
        if t.get("status") in ("queued", "running")
    )


def worker(task_id: str, req: JobReq):
    try:
        TASKS[task_id]["status"] = "running"

        result = run_once(
            mode=req.mode,
            overrides=req.overrides,
            do_export=req.do_export,
        )

        TASKS[task_id]["status"] = "done"
        TASKS[task_id]["result"] = result

    except Exception as e:
        TASKS[task_id]["status"] = "failed"
        TASKS[task_id]["error"] = str(e)


def _create_task(req: JobReq) -> str:
    """统一创建任务入口（给 create_job 和 rerun 共用）"""
    _apply_guardrails(req)

    if _active_jobs_count() >= MAX_ACTIVE_JOBS:
        raise HTTPException(429, f"too many active jobs (max {MAX_ACTIVE_JOBS})")

    task_id = str(uuid.uuid4())
    TASKS[task_id] = {
        "status": "queued",
        "result": None,
        "error": None,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "request": {
            "mode": req.mode,
            "overrides": req.overrides,
            "do_export": req.do_export,
        },
        # ✅ 记录来源（可选）：normal / rerun
        "source": "normal",
    }

    threading.Thread(target=worker, args=(task_id, req), daemon=True).start()
    return task_id


# ========================
# API
# ========================

@app.post("/jobs")
def create_job(req: JobReq):
    task_id = _create_task(req)
    return {"task_id": task_id}


@app.get("/jobs")
def list_jobs(limit: int = 20):
    """最近任务列表（给前端表格用）"""
    limit = max(1, min(int(limit), 200))

    def key_fn(tid: str):
        return TASKS[tid].get("created_at", "")

    ids = sorted(TASKS.keys(), key=key_fn, reverse=True)[:limit]

    items = []
    for tid in ids:
        t = TASKS[tid]
        items.append({
            "task_id": tid,
            "status": t.get("status"),
            "created_at": t.get("created_at"),
            "request": t.get("request"),
            "error": t.get("error"),
            "rows": (t.get("result") or {}).get("rows"),
            "export": (t.get("result") or {}).get("export"),
            "source": t.get("source", "normal"),
        })

    return {"items": items}


@app.get("/jobs/{task_id}")
def get_job(task_id: str):
    if task_id not in TASKS:
        raise HTTPException(404, "task_id not found")
    return TASKS[task_id]


@app.post("/jobs/{task_id}/rerun")
def rerun_job(task_id: str):
    """
    ✅ Rerun：用历史任务当时的参数，再创建一个新任务
    """
    old = TASKS.get(task_id)
    if not old:
        raise HTTPException(404, "task_id not found")

    req_obj = old.get("request") or {}
    mode = req_obj.get("mode", "dev")
    overrides = req_obj.get("overrides", {}) or {}
    do_export = bool(req_obj.get("do_export", True))

    new_req = JobReq(mode=mode, overrides=overrides, do_export=do_export)

    new_task_id = _create_task(new_req)
    TASKS[new_task_id]["source"] = f"rerun:{task_id}"

    return {"task_id": new_task_id}


@app.get("/jobs/{task_id}/download")
def download(task_id: str):
    task = TASKS.get(task_id)

    if not task or task["status"] != "done":
        raise HTTPException(404, "task not ready")

    export = (task.get("result") or {}).get("export") or {}
    zip_path = export.get("zip_path")
    if not zip_path:
        raise HTTPException(404, "zip not found")

    return FileResponse(zip_path, filename="ods.zip")