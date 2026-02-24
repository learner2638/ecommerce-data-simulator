# web/app.py
import os
import uuid
import random
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from config import Config
from pipeline import build_dataset
from check import check_head_item_consistency
from exporter import export_ods, write_hive_ddl, pack_ods_zip

app = FastAPI(title="DW Simulator Web")

JOB_ROOT = "out/jobs"


def _job_dir(job_id: str) -> str:
    return os.path.join(JOB_ROOT, job_id)


@app.get("/")
def home():
    return {"msg": "DW Simulator is running", "docs": "/docs"}


@app.post("/api/generate")
def generate(mode: Literal["dev", "prod"] = "dev", seed: Optional[int] = None):
    job_id = str(uuid.uuid4())
    job_dir = _job_dir(job_id)
    ods_dir = os.path.join(job_dir, "ods")
    os.makedirs(job_dir, exist_ok=True)

    cfg = Config(mode=mode)
    if seed is not None:
        cfg.seed = int(seed)
    rnd = random.Random(cfg.seed)

    ds = build_dataset(cfg, rnd)

    check_head_item_consistency(ds["orders"], ds["items"], sample_n=200)

    export_ods(ds, out_dir=ods_dir)

    ddl_path = write_hive_ddl(
        filepath=os.path.join(job_dir, "hive_ods_ddl.sql"),
        database="dw_ods"
    )

    zip_path = pack_ods_zip(ods_dir, zip_path=os.path.join(job_dir, "ods.zip"))

    return {
        "job_id": job_id,
        "mode": mode,
        "seed": cfg.seed,
        "download_zip": f"/api/download/{job_id}/ods.zip",
        "download_ddl": f"/api/download/{job_id}/hive_ods_ddl.sql",
    }


@app.get("/api/download/{job_id}/{filename}")
def download(job_id: str, filename: str):
    path = os.path.join(_job_dir(job_id), filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"file not found: {filename}")
    return FileResponse(path, filename=filename)