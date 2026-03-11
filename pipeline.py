from dims import gen_user_dim, gen_shop_dim, gen_sku_dim
from facts import (
    gen_orders,
    gen_order_items,
    prepare_order_context,
    prepare_item_context,
    gen_orders_batch,
    gen_order_items_batch,
)


def build_dataset(cfg, rnd):
    """
    全量内存模式：
    适合 dev / 小数据量调试
    """
    users = gen_user_dim(cfg, rnd)
    shops = gen_shop_dim(cfg, rnd)
    skus = gen_sku_dim(cfg, rnd)

    orders = gen_orders(cfg, rnd, shops=shops)
    items = gen_order_items(cfg, rnd, orders=orders, skus=skus)

    return {
        "users": users,
        "shops": shops,
        "skus": skus,
        "orders": orders,
        "items": items,
    }


def prepare_stream_context(cfg, rnd):
    """
    流式/批处理模式的上下文初始化：
    - 维表一次性生成
    - 抽样器一次性准备
    """
    users = gen_user_dim(cfg, rnd)
    shops = gen_shop_dim(cfg, rnd)
    skus = gen_sku_dim(cfg, rnd)

    order_ctx = prepare_order_context(cfg, rnd, shops)
    item_ctx = prepare_item_context(cfg, rnd, skus)

    return {
        "users": users,
        "shops": shops,
        "skus": skus,
        "order_ctx": order_ctx,
        "item_ctx": item_ctx,
    }


def iter_dataset_batches(cfg, rnd, batch_size):
    """
    批处理模式：
    按 batch 逐批产出 orders / items

    返回的每个 batch 结构：
    {
        "orders": [...],
        "items": [...],
        "start_oid": ...,
        "end_oid": ...,
    }
    """
    ctx = prepare_stream_context(cfg, rnd)

    order_item_id = 1
    total_orders = cfg.order_cnt

    for start_oid in range(1, total_orders + 1, batch_size):
        batch_orders = gen_orders_batch(
            cfg=cfg,
            rnd=rnd,
            ctx=ctx["order_ctx"],
            start_oid=start_oid,
            batch_size=batch_size,
        )

        batch_items, order_item_id = gen_order_items_batch(
            cfg=cfg,
            rnd=rnd,
            orders=batch_orders,
            item_ctx=ctx["item_ctx"],
            start_order_item_id=order_item_id,
        )

        end_oid = batch_orders[-1]["order_id"] if batch_orders else start_oid - 1

        yield {
            "orders": batch_orders,
            "items": batch_items,
            "start_oid": start_oid,
            "end_oid": end_oid,
        }


def iter_dataset_batches_range(
    cfg,
    rnd,
    batch_size,
    start_oid,
    end_oid,
    order_ctx=None,
    item_ctx=None,
    start_order_item_id=1,
):
    """
    只生成指定订单区间 [start_oid, end_oid]
    给多进程 worker 使用
    """
    if order_ctx is None or item_ctx is None:
        ctx = prepare_stream_context(cfg, rnd)
        order_ctx = ctx["order_ctx"]
        item_ctx = ctx["item_ctx"]

    next_order_item_id = start_order_item_id

    for current_start in range(start_oid, end_oid + 1, batch_size):
        current_batch_size = min(batch_size, end_oid - current_start + 1)

        batch_orders = gen_orders_batch(
            cfg=cfg,
            rnd=rnd,
            ctx=order_ctx,
            start_oid=current_start,
            batch_size=current_batch_size,
        )

        batch_items, next_order_item_id = gen_order_items_batch(
            cfg=cfg,
            rnd=rnd,
            orders=batch_orders,
            item_ctx=item_ctx,
            start_order_item_id=next_order_item_id,
        )

        current_end = batch_orders[-1]["order_id"] if batch_orders else current_start - 1

        yield {
            "orders": batch_orders,
            "items": batch_items,
            "start_oid": current_start,
            "end_oid": current_end,
            "next_order_item_id": next_order_item_id,
        }


def build_dataset_stream_meta(cfg, rnd):
    """
    给流式模式提供元信息：
    - 维表
    - 总订单数
    不返回全量 orders/items
    """
    ctx = prepare_stream_context(cfg, rnd)

    return {
        "users": ctx["users"],
        "shops": ctx["shops"],
        "skus": ctx["skus"],
        "total_orders": cfg.order_cnt,
        "order_ctx": ctx["order_ctx"],
        "item_ctx": ctx["item_ctx"],
    }