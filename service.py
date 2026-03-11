import os
import random
from collections import Counter
from multiprocessing import Pool

from config import Config
from pipeline import (
    build_dataset,
    prepare_stream_context,
    iter_dataset_batches,
    iter_dataset_batches_range,   # 需要你按我下面给的版本补到 pipeline.py
)
from exporter import (
    export_csv,
    init_csv,
    append_csv,
    ODS_SCHEMA,
    write_hive_ddl,
    pack_ods_zip,
)
from check import check_head_item_consistency


# =========================
# 原有：全量内存模式
# =========================
def run_once(mode="dev", overrides=None, do_export=True, sample_n=200):
    """
    全量内存模式：
    适合 dev / 小数据量调试
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
    paid_cnt = sum(
        1 for o in orders if o["status"] in ("PAID", "SHIPPED", "COMPLETED")
    )

    # Top
    top10_users = Counter(o["user_id"] for o in orders).most_common(10)
    top10_shops = Counter(o["shop_id"] for o in orders).most_common(10)
    top10_skus = Counter(it["sku_id"] for it in items).most_common(10)

    export_info = None
    if do_export:
        ods_dir = export_ods_full(ds, out_dir="out/ods")
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


def export_ods_full(ds, out_dir="out/ods"):
    """
    给 run_once 用的全量导出
    """
    os.makedirs(out_dir, exist_ok=True)

    export_csv(ds["users"], os.path.join(out_dir, "ods_user_dim.csv"), ODS_SCHEMA["ods_user_dim"])
    export_csv(ds["shops"], os.path.join(out_dir, "ods_shop_dim.csv"), ODS_SCHEMA["ods_shop_dim"])
    export_csv(ds["skus"], os.path.join(out_dir, "ods_sku_dim.csv"), ODS_SCHEMA["ods_sku_dim"])
    export_csv(ds["orders"], os.path.join(out_dir, "ods_orders.csv"), ODS_SCHEMA["ods_orders"])
    export_csv(ds["items"], os.path.join(out_dir, "ods_order_items.csv"), ODS_SCHEMA["ods_order_items"])

    return out_dir


# =========================
# 原有：单进程流式模式
# =========================
def run_once_stream(
    mode="prod",
    overrides=None,
    batch_size=50000,
    do_export=True,
    sample_n=200,
    progress_callback=None,
):
    """
    批处理模式：
    - 分批生成 orders / items
    - 边生成边写 CSV
    - 支持进度回调（给 API / 网页用）
    """
    cfg = Config(mode=mode, **(overrides or {}))
    rnd = random.Random(cfg.seed)

    # ============================
    # 1 初始化 pipeline 上下文
    # ============================
    ctx = prepare_stream_context(cfg, rnd)

    users = ctx["users"]
    shops = ctx["shops"]
    skus = ctx["skus"]

    ods_dir = "out/ods"
    os.makedirs(ods_dir, exist_ok=True)

    # ============================
    # 2 导出维表
    # ============================
    export_csv(users, os.path.join(ods_dir, "ods_user_dim.csv"), ODS_SCHEMA["ods_user_dim"])
    export_csv(shops, os.path.join(ods_dir, "ods_shop_dim.csv"), ODS_SCHEMA["ods_shop_dim"])
    export_csv(skus, os.path.join(ods_dir, "ods_sku_dim.csv"), ODS_SCHEMA["ods_sku_dim"])

    # ============================
    # 3 初始化事实表 CSV
    # ============================
    orders_path = os.path.join(ods_dir, "ods_orders.csv")
    items_path = os.path.join(ods_dir, "ods_order_items.csv")

    init_csv(orders_path, ODS_SCHEMA["ods_orders"])
    init_csv(items_path, ODS_SCHEMA["ods_order_items"])

    # ============================
    # 4 统计器
    # ============================
    cnt = Counter()
    refund_paid = 0
    paid_cnt = 0

    top_users = Counter()
    top_shops = Counter()
    top_skus = Counter()

    sample_orders = []
    sample_items = []

    total_orders = cfg.order_cnt
    total_items = 0

    # ============================
    # 5 分批生成
    # ============================
    for batch in iter_dataset_batches(cfg, rnd, batch_size):
        orders = batch["orders"]
        items = batch["items"]

        # 写入 CSV
        append_csv(orders, orders_path, ODS_SCHEMA["ods_orders"])
        append_csv(items, items_path, ODS_SCHEMA["ods_order_items"])

        total_items += len(items)

        # ======== 实时统计 ========
        cnt.update(o["status"] for o in orders)

        refund_paid += sum(
            1
            for o in orders
            if o.get("paid_amount", 0) > 0 and o.get("refund_amount", 0) > 0
        )

        paid_cnt += sum(
            1 for o in orders if o["status"] in ("PAID", "SHIPPED", "COMPLETED")
        )

        top_users.update(o["user_id"] for o in orders)
        top_shops.update(o["shop_id"] for o in orders)
        top_skus.update(it["sku_id"] for it in items)

        # ======== 抽样校验 ========
        if len(sample_orders) < sample_n:
            need = sample_n - len(sample_orders)
            sample_orders.extend(orders[:need])

        sampled_ids = {o["order_id"] for o in sample_orders}

        if len(sample_items) < sample_n * 5:
            for it in items:
                if it["order_id"] in sampled_ids:
                    sample_items.append(it)
                    if len(sample_items) >= sample_n * 5:
                        break

        # ======== 进度 ========
        done = batch["end_oid"]
        msg = f"{done}/{total_orders}, items={total_items}"
        print(f"[progress] {msg}")

        if progress_callback:
            progress_callback(done, total_orders, total_items)

    # ============================
    # 6 校验
    # ============================
    if sample_orders and sample_items:
        check_head_item_consistency(
            sample_orders,
            sample_items,
            sample_n=min(sample_n, len(sample_orders)),
        )

    # ============================
    # 7 导出
    # ============================
    export_info = None

    if do_export:
        ddl_path = write_hive_ddl("out/hive_ods_ddl.sql", database="dw_ods")
        zip_path = pack_ods_zip(ods_dir)

        export_info = {
            "ods_dir": ods_dir,
            "ddl_path": ddl_path,
            "zip_path": zip_path,
        }

    # ============================
    # 8 返回统计
    # ============================
    return {
        "cfg": cfg.to_dict() if hasattr(cfg, "to_dict") else cfg.__dict__,
        "rows": {
            "users": len(users),
            "shops": len(shops),
            "skus": len(skus),
            "orders": total_orders,
            "items": total_items,
        },
        "status_dist": dict(cnt),
        "paid_ratio": round(paid_cnt / max(total_orders, 1), 6),
        "refund_ratio_in_paid": round(refund_paid / max(paid_cnt, 1), 6),
        "top10_users": top_users.most_common(10),
        "top10_shops": top_shops.most_common(10),
        "top10_skus": top_skus.most_common(10),
        "export": export_info,
    }


# =========================
# 多进程工具函数
# =========================
def _split_order_ranges(total_orders, workers):
    ranges = []
    chunk = total_orders // workers
    start = 1

    for i in range(workers):
        end = start + chunk - 1
        if i == workers - 1:
            end = total_orders
        ranges.append((start, end))
        start = end + 1

    return ranges


def _merge_csv_files(part_files, final_file):
    os.makedirs(os.path.dirname(final_file), exist_ok=True)

    first_file = True
    with open(final_file, "w", encoding="utf-8", newline="") as fout:
        for path in sorted(part_files):
            with open(path, "r", encoding="utf-8") as fin:
                for idx, line in enumerate(fin):
                    if not first_file and idx == 0:
                        continue
                    fout.write(line)
            first_file = False


def _cleanup_files(paths):
    for path in paths:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass


def _cleanup_dir_if_empty(path):
    try:
        if os.path.isdir(path) and not os.listdir(path):
            os.rmdir(path)
    except Exception:
        pass


# =========================
# 多进程 worker
# =========================
def _stream_worker(args):
    """
    每个 worker 负责一个订单区间：
    - 独立初始化随机数
    - 独立生成 part CSV
    - 返回局部统计
    """
    (
        worker_id,
        mode,
        overrides,
        batch_size,
        start_oid,
        end_oid,
        parts_dir,
    ) = args

    cfg = Config(mode=mode, **(overrides or {}))
    rnd = random.Random(cfg.seed + worker_id)

    ctx = prepare_stream_context(cfg, rnd)

    orders_path = os.path.join(parts_dir, f"ods_orders_part_{worker_id:03d}.csv")
    items_path = os.path.join(parts_dir, f"ods_order_items_part_{worker_id:03d}.csv")

    init_csv(orders_path, ODS_SCHEMA["ods_orders"])
    init_csv(items_path, ODS_SCHEMA["ods_order_items"])

    cnt = Counter()
    refund_paid = 0
    paid_cnt = 0

    top_users = Counter()
    top_shops = Counter()
    top_skus = Counter()

    total_orders_done = 0
    total_items = 0

    # 给每个 worker 一个独立的大号 item id 段，避免重复
    start_order_item_id_base = worker_id * 10_000_000_000 + 1
    next_order_item_id = start_order_item_id_base

    for batch in iter_dataset_batches_range(
        cfg=cfg,
        rnd=rnd,
        batch_size=batch_size,
        start_oid=start_oid,
        end_oid=end_oid,
        order_ctx=ctx["order_ctx"],
        item_ctx=ctx["item_ctx"],
        start_order_item_id=next_order_item_id,
    ):
        orders = batch["orders"]
        items = batch["items"]

        append_csv(orders, orders_path, ODS_SCHEMA["ods_orders"])
        append_csv(items, items_path, ODS_SCHEMA["ods_order_items"])

        total_orders_done += len(orders)
        total_items += len(items)

        next_order_item_id = batch["next_order_item_id"]

        cnt.update(o["status"] for o in orders)

        refund_paid += sum(
            1
            for o in orders
            if o.get("paid_amount", 0) > 0 and o.get("refund_amount", 0) > 0
        )

        paid_cnt += sum(
            1 for o in orders if o["status"] in ("PAID", "SHIPPED", "COMPLETED")
        )

        top_users.update(o["user_id"] for o in orders)
        top_shops.update(o["shop_id"] for o in orders)
        top_skus.update(it["sku_id"] for it in items)

        print(f"[worker-{worker_id}] {batch['end_oid']}/{end_oid}, items={total_items}")

    return {
        "worker_id": worker_id,
        "orders_path": orders_path,
        "items_path": items_path,
        "rows": {
            "orders": total_orders_done,
            "items": total_items,
        },
        "status_dist": dict(cnt),
        "refund_paid": refund_paid,
        "paid_cnt": paid_cnt,
        "top_users": dict(top_users),
        "top_shops": dict(top_shops),
        "top_skus": dict(top_skus),
    }


# =========================
# 新增：多进程流式模式
# =========================
def run_once_stream_parallel(
    mode="prod",
    overrides=None,
    batch_size=100000,
    do_export=True,
    workers=6,
    keep_parts=False,
):
    """
    多进程流式模式：
    - 维表主进程一次性导出
    - 订单按区间切分给多个 worker
    - 每个 worker 生成自己的 part 文件
    - 主进程最终合并 CSV
    """
    cfg = Config(mode=mode, **(overrides or {}))
    rnd = random.Random(cfg.seed)

    # ============================
    # 1 主进程生成维表并导出
    # ============================
    ctx = prepare_stream_context(cfg, rnd)

    users = ctx["users"]
    shops = ctx["shops"]
    skus = ctx["skus"]

    ods_dir = "out/ods"
    parts_dir = "out/ods_parts"

    os.makedirs(ods_dir, exist_ok=True)
    os.makedirs(parts_dir, exist_ok=True)

    export_csv(users, os.path.join(ods_dir, "ods_user_dim.csv"), ODS_SCHEMA["ods_user_dim"])
    export_csv(shops, os.path.join(ods_dir, "ods_shop_dim.csv"), ODS_SCHEMA["ods_shop_dim"])
    export_csv(skus, os.path.join(ods_dir, "ods_sku_dim.csv"), ODS_SCHEMA["ods_sku_dim"])

    # ============================
    # 2 切分订单区间
    # ============================
    ranges = _split_order_ranges(cfg.order_cnt, workers)
    tasks = []

    for worker_id, (start_oid, end_oid) in enumerate(ranges):
        tasks.append(
            (
                worker_id,
                mode,
                overrides,
                batch_size,
                start_oid,
                end_oid,
                parts_dir,
            )
        )

    # ============================
    # 3 并行执行
    # ============================
    with Pool(processes=workers) as pool:
        results = pool.map(_stream_worker, tasks)

    # ============================
    # 4 合并 part 文件
    # ============================
    orders_parts = [r["orders_path"] for r in results]
    items_parts = [r["items_path"] for r in results]

    final_orders_path = os.path.join(ods_dir, "ods_orders.csv")
    final_items_path = os.path.join(ods_dir, "ods_order_items.csv")

    _merge_csv_files(orders_parts, final_orders_path)
    _merge_csv_files(items_parts, final_items_path)

    # ============================
    # 5 汇总统计
    # ============================
    cnt = Counter()
    refund_paid = 0
    paid_cnt = 0
    top_users = Counter()
    top_shops = Counter()
    top_skus = Counter()

    total_orders = 0
    total_items = 0

    for r in results:
        total_orders += r["rows"]["orders"]
        total_items += r["rows"]["items"]

        cnt.update(r["status_dist"])
        refund_paid += r["refund_paid"]
        paid_cnt += r["paid_cnt"]

        top_users.update(r["top_users"])
        top_shops.update(r["top_shops"])
        top_skus.update(r["top_skus"])

    # ============================
    # 6 导出
    # ============================
    export_info = None
    if do_export:
        ddl_path = write_hive_ddl("out/hive_ods_ddl.sql", database="dw_ods")
        zip_path = pack_ods_zip(ods_dir)
        export_info = {
            "ods_dir": ods_dir,
            "ddl_path": ddl_path,
            "zip_path": zip_path,
        }

    # ============================
    # 7 清理 part 文件
    # ============================
    if not keep_parts:
        _cleanup_files(orders_parts + items_parts)
        _cleanup_dir_if_empty(parts_dir)

    # ============================
    # 8 返回统计
    # ============================
    return {
        "cfg": cfg.to_dict() if hasattr(cfg, "to_dict") else cfg.__dict__,
        "rows": {
            "users": len(users),
            "shops": len(shops),
            "skus": len(skus),
            "orders": total_orders,
            "items": total_items,
        },
        "status_dist": dict(cnt),
        "paid_ratio": round(paid_cnt / max(total_orders, 1), 6),
        "refund_ratio_in_paid": round(refund_paid / max(paid_cnt, 1), 6),
        "top10_users": top_users.most_common(10),
        "top10_shops": top_shops.most_common(10),
        "top10_skus": top_skus.most_common(10),
        "export": export_info,
    }