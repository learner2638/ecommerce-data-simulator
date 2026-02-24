# facts.py
from datetime import timedelta

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
# 1) 店铺：帕累托（头部店更强）
# =========================
def _build_shop_sampler(shops, power=1.15):
    """
    在 shop_weight 基础上做幂次放大：weight^power
    power=1.0 表示不放大；越大头部越集中。
    """
    shop_ids = [s["shop_id"] for s in shops]
    shop_weights = [(s["shop_weight"] ** power) for s in shops]
    return shop_ids, shop_weights

# =========================
# 2) SKU：爆品/长尾热度（按类目内rank权重）
# =========================
def _build_sku_map(skus):
    return {s["sku_id"]: s for s in skus}

def _build_category_sku_rank_sampler(cfg, rnd, skus):
    """
    为每个类目构造一个“爆品长尾”的抽样器：
    category -> (sku_ids_in_cat, weights_in_cat)
    权重用 1/(rank^beta)，beta 越大越爆品集中。
    """
    beta = 1.10  # 1.05~1.25，越大爆品越集中
    # category -> list[sku_id]
    cat_pool = {}
    for s in skus:
        cat_pool.setdefault(s["category"], []).append(s["sku_id"])

    cat_sampler = {}
    for cat, sku_ids in cat_pool.items():
        # 为了稳定可复现：按 sku_id 排序当作 rank
        sku_ids_sorted = sorted(sku_ids)
        weights = [1.0 / ((i + 1) ** beta) for i in range(len(sku_ids_sorted))]
        cat_sampler[cat] = (sku_ids_sorted, weights)

    return cat_sampler

# =========================
# 3) 用户：帕累托 + 复购时间
# =========================
def _build_user_sampler(cfg):
    """
    Zipf风格用户权重：1/(rank^alpha)
    alpha 越大头部越集中。你之前 1.2 很猛，这里降温到 1.05 更像主流电商。
    """
    alpha = 1.05
    user_ids = list(range(1, cfg.user_cnt + 1))
    weights = [1.0 / ((i + 1) ** alpha) for i in range(cfg.user_cnt)]
    return user_ids, weights

def _build_user_time_profile(cfg, rnd):
    """
    heavy/normal/light 三段人群：决定下单更密集还是更稀疏
    """
    profiles = {}
    for uid in range(1, cfg.user_cnt + 1):
        r = rnd.random()
        if r < 0.12:
            seg = "heavy"
            gap_mu = 9
        elif r < 0.70:
            seg = "normal"
            gap_mu = 22
        else:
            seg = "light"
            gap_mu = 55

        gap_mu = max(2, int(gap_mu * (0.7 + rnd.random() * 0.6)))
        profiles[uid] = {"seg": seg, "gap_mu_days": gap_mu}
    return profiles

def _sample_created_time_for_user(cfg, rnd, user_profiles, uid):
    now = cfg.base_time
    mu = user_profiles[uid]["gap_mu_days"]

    # 偏向最近：mu 越小越容易抽到更近
    days_ago = rnd.randint(0, min(cfg.days_back, mu * 4))
    minute_offset = rnd.randint(0, 24 * 60 - 1)
    return now - timedelta(days=days_ago, minutes=minute_offset)

# =========================
# 4) 生成 orders
# =========================
def gen_orders(cfg, rnd, shops):
    orders = []

    # 店铺抽样器：帕累托放大
    shop_ids, shop_weights = _build_shop_sampler(shops, power=1.15)

    # 用户抽样器：帕累托降温版
    user_ids, user_weights = _build_user_sampler(cfg)
    user_profiles = _build_user_time_profile(cfg, rnd)

    for oid in range(1, cfg.order_cnt + 1):
        user_id = rnd.choices(user_ids, weights=user_weights, k=1)[0]
        shop_id = rnd.choices(shop_ids, weights=shop_weights, k=1)[0]
        created_time = _sample_created_time_for_user(cfg, rnd, user_profiles, user_id)

        base_status = _pick_base_status(cfg, rnd)

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
# 5) 生成 items + 回填履约/退款
# =========================
def gen_order_items(cfg, rnd, orders, skus):
    items = []
    order_item_id = 1

    sku_map = _build_sku_map(skus)
    # 类目内爆品长尾抽样器
    cat_sku_sampler = _build_category_sku_rank_sampler(cfg, rnd, skus)

    cats = list(cfg.category_buy_weights.keys())
    cat_weights = [cfg.category_buy_weights[c] for c in cats]

    # ===== A: 明细 & 回填 total =====
    for o in orders:
        order_id = o["order_id"]
        user_id = o["user_id"]
        shop_id = o["shop_id"]

        run_len = rnd.randint(cfg.runlen_min, cfg.runlen_max)
        chosen_skus = set()

        total_qty = 0
        total_amount = 0

        for _ in range(run_len):
            # 1) 按类目购买权重抽类目
            cat = rnd.choices(cats, weights=cat_weights, k=1)[0]

            # 2) 在该类目内按爆品长尾权重抽 sku_id
            sku_ids_in_cat, w_in_cat = cat_sku_sampler[cat]
            sku_id = rnd.choices(sku_ids_in_cat, weights=w_in_cat, k=1)[0]

            # 3) 单内不重复（极端情况做降级）
            guard = 0
            while sku_id in chosen_skus:
                sku_id = rnd.choices(sku_ids_in_cat, weights=w_in_cat, k=1)[0]
                guard += 1
                if guard > 30:
                    sku_id = rnd.randint(1, cfg.sku_cnt)
                    break
            chosen_skus.add(sku_id)

            item_qty = rnd.randint(cfg.qty_min, cfg.qty_max)
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
    for o in orders:
        created = o["created_time"]
        total = o["total_amount"]
        status = o["status"]

        discount_rate = rnd.uniform(cfg.discount_rate_min, cfg.discount_rate_max)
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
            delay_min = rnd.randint(cfg.cancel_delay_min_min, cfg.cancel_delay_max_min)
            cancel_time = created + timedelta(minutes=delay_min)

        elif status == "PAID":
            pay_delay = rnd.randint(cfg.pay_delay_min_min, cfg.pay_delay_max_min)
            pay_time = created + timedelta(minutes=pay_delay)

            paid = total - discount
            if paid < 0:
                paid = 0

            # 履约
            shipped = (rnd.random() < cfg.p_ship_given_paid)
            if shipped:
                ship_delay = rnd.randint(cfg.ship_delay_min_min, cfg.ship_delay_max_min)
                ship_time = pay_time + timedelta(minutes=ship_delay)
                o["status"] = "SHIPPED"

                completed = (rnd.random() < cfg.p_complete_given_shipped)
                if completed:
                    comp_delay = rnd.randint(cfg.complete_delay_min_min, cfg.complete_delay_max_min)
                    complete_time = ship_time + timedelta(minutes=comp_delay)
                    o["status"] = "COMPLETED"

            # 退款
            if rnd.random() < cfg.p_refund_given_paid and paid > 0:
                if rnd.random() < cfg.p_refund_full:
                    refund_type = "FULL"
                    refund = paid
                else:
                    refund_type = "PARTIAL"
                    refund_rate = rnd.uniform(cfg.refund_rate_min, cfg.refund_rate_max)
                    refund = _int_money(paid * refund_rate)
                    if refund > paid:
                        refund = paid

                stage_r = rnd.random()
                if stage_r < cfg.p_refund_stage_after_pay or ship_time is None:
                    base_time = pay_time
                elif stage_r < cfg.p_refund_stage_after_pay + cfg.p_refund_stage_after_ship or complete_time is None:
                    base_time = ship_time
                else:
                    base_time = complete_time

                rdelay = rnd.randint(cfg.refund_delay_min_min, cfg.refund_delay_max_min)
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

    return items