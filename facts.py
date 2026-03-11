from bisect import bisect_left
from datetime import timedelta
from itertools import accumulate


def _int_money(x):
    return int(round(x))


def _pick_base_status(cfg, rnd):
    r = rnd.random()
    if r < cfg.p_unpaid:
        return "UNPAID"
    elif r < cfg.p_unpaid + cfg.p_paid:
        return "PAID"
    else:
        return "CANCELLED"


# =========================
# 0) 通用：累计权重采样器
# =========================
def _build_cum_sampler(ids, weights):
    cum_weights = list(accumulate(weights))
    total_weight = cum_weights[-1]
    return {
        "ids": ids,
        "cum_weights": cum_weights,
        "total_weight": total_weight,
    }


def _sample_one(rnd, sampler):
    x = rnd.random() * sampler["total_weight"]
    idx = bisect_left(sampler["cum_weights"], x)
    return sampler["ids"][idx]


# =========================
# 1) 店铺：帕累托（头部店更强）
# =========================
def _build_shop_sampler(shops, power=1.15):
    """
    在 shop_weight 基础上做幂次放大：weight^power
    power=1.0 表示不放大；越大头部越集中。
    """
    shop_ids = [s["shop_id"] for s in shops]
    shop_weights = [(s["shop_weight"] ** power) for s in shops]
    return _build_cum_sampler(shop_ids, shop_weights)


# =========================
# 2) SKU：爆品/长尾热度（按类目内rank权重）
# =========================
def _build_sku_map(skus):
    return {s["sku_id"]: s for s in skus}


def _build_category_sku_rank_sampler(cfg, rnd, skus):
    """
    为每个类目构造一个“爆品长尾”的抽样器：
    category -> sampler
    权重用 1/(rank^beta)，beta 越大越爆品集中。
    """
    beta = 1.10
    cat_pool = {}
    for s in skus:
        cat_pool.setdefault(s["category"], []).append(s["sku_id"])

    cat_sampler = {}
    for cat, sku_ids in cat_pool.items():
        sku_ids_sorted = sorted(sku_ids)
        weights = [1.0 / ((i + 1) ** beta) for i in range(len(sku_ids_sorted))]
        cat_sampler[cat] = _build_cum_sampler(sku_ids_sorted, weights)

    return cat_sampler


# =========================
# 3) 用户：帕累托 + 复购时间
# =========================
def _build_user_sampler(cfg):
    """
    Zipf风格用户权重：1/(rank^alpha)
    """
    alpha = 1.05
    user_ids = list(range(1, cfg.user_cnt + 1))
    weights = [1.0 / ((i + 1) ** alpha) for i in range(cfg.user_cnt)]
    return _build_cum_sampler(user_ids, weights)


def _build_user_time_profile(cfg, rnd):
    """
    heavy/normal/light 三段人群：决定下单更密集还是更稀疏
    """
    profiles = {}
    rnd_random = rnd.random

    for uid in range(1, cfg.user_cnt + 1):
        r = rnd_random()
        if r < 0.12:
            seg = "heavy"
            gap_mu = 9
        elif r < 0.70:
            seg = "normal"
            gap_mu = 22
        else:
            seg = "light"
            gap_mu = 55

        gap_mu = max(2, int(gap_mu * (0.7 + rnd_random() * 0.6)))
        profiles[uid] = {"seg": seg, "gap_mu_days": gap_mu}
    return profiles


def _sample_created_time_for_user(cfg, rnd, user_profiles, uid):
    now = cfg.base_time
    mu = user_profiles[uid]["gap_mu_days"]

    days_ago = rnd.randint(0, min(cfg.days_back, mu * 4))
    minute_offset = rnd.randint(0, 24 * 60 - 1)
    return now - timedelta(days=days_ago, minutes=minute_offset)


# =========================
# 4) 为批处理预先准备上下文
# =========================
def prepare_order_context(cfg, rnd, shops):
    shop_sampler = _build_shop_sampler(shops, power=1.15)
    user_sampler = _build_user_sampler(cfg)
    user_profiles = _build_user_time_profile(cfg, rnd)

    return {
        "shop_sampler": shop_sampler,
        "user_sampler": user_sampler,
        "user_profiles": user_profiles,
    }


def prepare_item_context(cfg, rnd, skus):
    sku_map = _build_sku_map(skus)
    cat_sku_sampler = _build_category_sku_rank_sampler(cfg, rnd, skus)

    cats = list(cfg.category_buy_weights.keys())
    cat_weights = [cfg.category_buy_weights[c] for c in cats]
    cat_sampler = _build_cum_sampler(cats, cat_weights)

    return {
        "sku_map": sku_map,
        "cat_sku_sampler": cat_sku_sampler,
        "cat_sampler": cat_sampler,
    }


# =========================
# 5) 单批生成 orders
# =========================
def gen_orders_batch(cfg, rnd, ctx, start_oid, batch_size):
    orders = []

    shop_sampler = ctx["shop_sampler"]
    user_sampler = ctx["user_sampler"]
    user_profiles = ctx["user_profiles"]

    end_oid = min(start_oid + batch_size, cfg.order_cnt + 1)

    sample_one = _sample_one
    sample_created = _sample_created_time_for_user
    pick_status = _pick_base_status

    for oid in range(start_oid, end_oid):
        user_id = sample_one(rnd, user_sampler)
        shop_id = sample_one(rnd, shop_sampler)
        created_time = sample_created(cfg, rnd, user_profiles, user_id)
        base_status = pick_status(cfg, rnd)

        orders.append({
            "order_id": oid,
            "user_id": user_id,
            "shop_id": shop_id,
            "created_time": created_time,
            "status": base_status,

            "total_qty": 0,
            "total_amount": 0,

            "discount_amount": 0,
            "paid_amount": 0,
            "refund_amount": 0,

            "pay_time": None,
            "cancel_time": None,
            "ship_time": None,
            "complete_time": None,
            "refund_time": None,

            "refund_type": "NONE",
        })

    return orders


# =========================
# 6) 单批生成 items + 回填订单金额/履约
# =========================
def gen_order_items_batch(cfg, rnd, orders, item_ctx, start_order_item_id):
    items = []
    order_item_id = start_order_item_id

    sku_map = item_ctx["sku_map"]
    cat_sku_sampler = item_ctx["cat_sku_sampler"]
    cat_sampler = item_ctx["cat_sampler"]

    rnd_randint = rnd.randint
    rnd_random = rnd.random
    rnd_uniform = rnd.uniform
    sample_one = _sample_one

    qty_min = cfg.qty_min
    qty_max = cfg.qty_max
    runlen_min = cfg.runlen_min
    runlen_max = cfg.runlen_max
    sku_cnt = cfg.sku_cnt

    # ===== A: 明细 & 回填 total =====
    for o in orders:
        order_id = o["order_id"]
        user_id = o["user_id"]
        shop_id = o["shop_id"]

        run_len = rnd_randint(runlen_min, runlen_max)
        chosen_skus = set()

        total_qty = 0
        total_amount = 0

        for _ in range(run_len):
            cat = sample_one(rnd, cat_sampler)
            sku_sampler = cat_sku_sampler[cat]
            sku_id = sample_one(rnd, sku_sampler)

            guard = 0
            while sku_id in chosen_skus:
                sku_id = sample_one(rnd, sku_sampler)
                guard += 1
                if guard > 30:
                    sku_id = rnd_randint(1, sku_cnt)
                    break

            chosen_skus.add(sku_id)

            item_qty = rnd_randint(qty_min, qty_max)
            sku_price = sku_map[sku_id]["sku_price"]
            item_amount = sku_price * item_qty

            items.append({
                "order_item_id": order_item_id,
                "order_id": order_id,
                "user_id": user_id,
                "shop_id": shop_id,
                "sku_id": sku_id,
                "item_qty": item_qty,
                "sku_price": sku_price,
                "item_amount": item_amount,
            })
            order_item_id += 1

            total_qty += item_qty
            total_amount += item_amount

        o["total_qty"] = total_qty
        o["total_amount"] = total_amount

    # ===== B: 回填金额/时间/履约/退款 =====
    discount_rate_min = cfg.discount_rate_min
    discount_rate_max = cfg.discount_rate_max

    cancel_delay_min_min = cfg.cancel_delay_min_min
    cancel_delay_max_min = cfg.cancel_delay_max_min

    pay_delay_min_min = cfg.pay_delay_min_min
    pay_delay_max_min = cfg.pay_delay_max_min

    ship_delay_min_min = cfg.ship_delay_min_min
    ship_delay_max_min = cfg.ship_delay_max_min

    complete_delay_min_min = cfg.complete_delay_min_min
    complete_delay_max_min = cfg.complete_delay_max_min

    refund_delay_min_min = cfg.refund_delay_min_min
    refund_delay_max_min = cfg.refund_delay_max_min

    refund_rate_min = cfg.refund_rate_min
    refund_rate_max = cfg.refund_rate_max

    p_ship_given_paid = cfg.p_ship_given_paid
    p_complete_given_shipped = cfg.p_complete_given_shipped
    p_refund_given_paid = cfg.p_refund_given_paid
    p_refund_full = cfg.p_refund_full
    p_refund_stage_after_pay = cfg.p_refund_stage_after_pay
    p_refund_stage_after_ship = cfg.p_refund_stage_after_ship

    for o in orders:
        created = o["created_time"]
        total = o["total_amount"]
        status = o["status"]

        discount_rate = rnd_uniform(discount_rate_min, discount_rate_max)
        discount = _int_money(total * discount_rate)
        if discount > total:
            discount = total

        paid = 0
        refund = 0

        pay_time = None
        cancel_time = None
        ship_time = None
        complete_time = None
        refund_time = None
        refund_type = "NONE"

        if status == "UNPAID":
            pass

        elif status == "CANCELLED":
            delay_min = rnd_randint(cancel_delay_min_min, cancel_delay_max_min)
            cancel_time = created + timedelta(minutes=delay_min)

        elif status == "PAID":
            pay_delay = rnd_randint(pay_delay_min_min, pay_delay_max_min)
            pay_time = created + timedelta(minutes=pay_delay)

            paid = total - discount
            if paid < 0:
                paid = 0

            shipped = (rnd_random() < p_ship_given_paid)
            if shipped:
                ship_delay = rnd_randint(ship_delay_min_min, ship_delay_max_min)
                ship_time = pay_time + timedelta(minutes=ship_delay)
                o["status"] = "SHIPPED"

                completed = (rnd_random() < p_complete_given_shipped)
                if completed:
                    comp_delay = rnd_randint(complete_delay_min_min, complete_delay_max_min)
                    complete_time = ship_time + timedelta(minutes=comp_delay)
                    o["status"] = "COMPLETED"

            if rnd_random() < p_refund_given_paid and paid > 0:
                if rnd_random() < p_refund_full:
                    refund_type = "FULL"
                    refund = paid
                else:
                    refund_type = "PARTIAL"
                    refund_rate = rnd_uniform(refund_rate_min, refund_rate_max)
                    refund = _int_money(paid * refund_rate)
                    if refund > paid:
                        refund = paid

                stage_r = rnd_random()
                if stage_r < p_refund_stage_after_pay or ship_time is None:
                    base_time = pay_time
                elif stage_r < p_refund_stage_after_pay + p_refund_stage_after_ship or complete_time is None:
                    base_time = ship_time
                else:
                    base_time = complete_time

                rdelay = rnd_randint(refund_delay_min_min, refund_delay_max_min)
                refund_time = base_time + timedelta(minutes=rdelay)

        else:
            raise ValueError(f"unknown status: {status}")

        o["discount_amount"] = discount
        o["paid_amount"] = paid
        o["refund_amount"] = refund

        o["pay_time"] = pay_time
        o["cancel_time"] = cancel_time
        o["ship_time"] = ship_time
        o["complete_time"] = complete_time
        o["refund_time"] = refund_time
        o["refund_type"] = refund_type

    return items, order_item_id


# =========================
# 7) 兼容旧版：全量生成 orders
# =========================
def gen_orders(cfg, rnd, shops):
    ctx = prepare_order_context(cfg, rnd, shops)
    return gen_orders_batch(cfg, rnd, ctx, start_oid=1, batch_size=cfg.order_cnt)


# =========================
# 8) 兼容旧版：全量生成 items
# =========================
def gen_order_items(cfg, rnd, orders, skus):
    item_ctx = prepare_item_context(cfg, rnd, skus)
    items, _ = gen_order_items_batch(cfg, rnd, orders, item_ctx, start_order_item_id=1)
    return items


