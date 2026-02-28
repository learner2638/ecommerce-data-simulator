# service.py
import random
from collections import Counter

from config import Config
from pipeline import build_dataset
from check import check_head_item_consistency


def run_once(mode="dev", overrides=None, do_export=True, sample_n=200):
    """
    mode: dev/prod 只是预设
    overrides: dict，想改哪个参数就传哪个（单个/多个都行）
    do_export: 是否导出 ods/ddl/zip
    """
    cfg = Config(mode=mode, **(overrides or {}))
    rnd = random.Random(cfg.seed)

    ds = build_dataset(cfg, rnd)

    orders = ds["orders"]
    items = ds["items"]

    # 校验
    check_head_item_consistency(orders, items, sample_n=sample_n)

    # 状态分布 + 退款率
    cnt = Counter(o["status"] for o in orders)
    refund_paid = sum(
        1 for o in orders
        if o.get("paid_amount", 0) > 0 and o.get("refund_amount", 0) > 0
    )
    paid_cnt = cnt.get("PAID", 0) + cnt.get("SHIPPED", 0) + cnt.get("COMPLETED", 0)

    # Top
    top10_users = Counter(o["user_id"] for o in orders).most_common(10)
    top10_shops = Counter(o["shop_id"] for o in orders).most_common(10)
    top10_skus  = Counter(it["sku_id"] for it in items).most_common(10)

    export_info = None
    if do_export:
        from exporter import export_ods, write_hive_ddl, pack_ods_zip
        ods_dir = export_ods(ds, out_dir="out/ods")
        ddl_path = write_hive_ddl("out/hive_ods_ddl.sql", database="dw_ods")
        zip_path = pack_ods_zip(ods_dir)
        export_info = {"ods_dir": ods_dir, "ddl_path": ddl_path, "zip_path": zip_path}

    return {
        "cfg": cfg.to_dict() if hasattr(cfg, "to_dict") else cfg.__dict__,
        "rows": {
            "users": len(ds["users"]),
            "shops": len(ds["shops"]),
            "skus": len(ds["skus"]),
            "orders": len(ds["orders"]),
            "items": len(ds["items"]),
        },
        "status_dist": dict(cnt),
        "paid_ratio": round(paid_cnt / max(len(orders), 1), 6),
        "refund_ratio_in_paid": round(refund_paid / max(paid_cnt, 1), 6),
        "top10_users": top10_users,
        "top10_shops": top10_shops,
        "top10_skus": top10_skus,
        "export": export_info,
    }