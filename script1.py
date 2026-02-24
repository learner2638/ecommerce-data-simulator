a

def logistic_days(shop_type: str, rnd: random.Random):
    # 按店铺类型给不同物流分布（可再工程化）
    if shop_type == "L":
        return rnd.randint(1, 3)
    if shop_type == "M":
        return rnd.randint(2, 5)
    return rnd.randint(3, 8)


# ----------------------------
# Facts
# ----------------------------
def gen_orders_and_items(cfg: GenConfig, df_sku, df_shop, rnd: random.Random):
    # 加速：把 sku_price 做成数组，sku_id 从 1..sku_cnt
    sku_price = df_sku["sku_price"].to_list()

    shop_ids = df_shop["shop_id"].to_list()
    shop_weights = df_shop["shop_weight"].to_list()
    shop_type_map = dict(zip(df_shop["shop_id"], df_shop["shop_type"]))
    shop_sampler = WeightedSampler(shop_ids, shop_weights)

    orders = []
    items = []

    for _ in range(cfg.order_cnt):
        order_id = str(uuid.uuid4())
        user_id = rnd.randint(1, cfg.user_cnt)
        shop_id = shop_sampler.sample(rnd)
        shop_type = shop_type_map[shop_id]

        status = pick_status(cfg, rnd)
        created_time = rand_time_in_days(cfg, rnd)

        paid_time = cancel_time = refund_time = None
        if status in ("PAID", "REFUNDED_PART", "REFUNDED_FULL"):
            paid_time = created_time + timedelta(minutes=rnd.randint(1, 120))
        if status == "CANCELLED":
            cancel_time = created_time + timedelta(minutes=rnd.randint(5, 180))
        if status in ("REFUNDED_PART", "REFUNDED_FULL"):
            refund_time = paid_time + timedelta(days=rnd.randint(1, 20))

        # 生成明细
        run_len = sample_runlen(cfg, rnd)
        # sku 尽量不重复：sample
        sku_ids = rnd.sample(range(1, cfg.sku_cnt + 1), k=run_len)

        gross_amount = 0
        sum_qty = 0
        for idx, sid in enumerate(sku_ids):
            qty = rnd.randint(1, 5)
            price = sku_price[sid - 1]
            line_gross = price * qty
            gross_amount += line_gross
            sum_qty += qty

            it_created = created_time + timedelta(seconds=rnd.randint(0, 180))
            recv = it_created + timedelta(days=logistic_days(shop_type, rnd))

            items.append({
                "order_item_id": str(uuid.uuid4()),
                "order_id": order_id,
                "user_id": user_id,
                "shop_id": shop_id,
                "sku_id": sid,
                "item_qty": qty,
                "payable_amount": line_gross,   # 明细先不含券/运费，保证可汇总
                "created_time": it_created,
                "received_time": recv
            })

        # 订单级规则
        discount_rate = rnd.random() * (cfg.discount_rate_max - cfg.discount_rate_min) + cfg.discount_rate_min
        discount_amount = int(round(gross_amount * discount_rate))
        coupon_amount = rnd.randint(cfg.coupon_min, cfg.coupon_max)
        freight_amount = rnd.randint(cfg.freight_min, cfg.freight_max)

        receivable_amount = gross_amount - discount_amount - coupon_amount + freight_amount
        if receivable_amount < 0:
            receivable_amount = 0

        paid_amount = receivable_amount if status in ("PAID", "REFUNDED_PART", "REFUNDED_FULL") else 0

        if status == "REFUNDED_FULL":
            refund_amount = paid_amount
        elif status == "REFUNDED_PART":
            rr = rnd.random() * (cfg.refund_part_max - cfg.refund_part_min) + cfg.refund_part_min
            refund_amount = int(round(paid_amount * rr))
        else:
            refund_amount = 0

        net_paid_amount = paid_amount - refund_amount

        orders.append({
            "order_id": order_id,
            "user_id": user_id,
            "shop_id": shop_id,
            "status": status,
            "created_time": created_time,
            "paid_time": paid_time,
            "cancel_time": cancel_time,
            "refund_time": refund_time,

            "gross_amount": gross_amount,
            "discount_rate": round(discount_rate, 4),
            "discount_amount": discount_amount,
            "coupon_amount": coupon_amount,
            "freight_amount": freight_amount,
            "receivable_amount": receivable_amount,
            "paid_amount": paid_amount,
            "refund_amount": refund_amount,
            "net_paid_amount": net_paid_amount,

            "sum_item_qty": sum_qty,
            "distinct_sku_cnt": run_len
        })

    return pd.DataFrame(orders), pd.DataFrame(items)


# ----------------------------
# Checks
# ----------------------------
def check_consistency(df_orders, df_items):
    agg = df_items.groupby("order_id", as_index=False)["payable_amount"].sum().rename(columns={"payable_amount": "item_sum"})
    m = df_orders.merge(agg, on="order_id", how="left")
    m["diff"] = m["gross_amount"] - m["item_sum"]
    bad_amount = m[m["diff"] != 0]

    # paid/refund 规则
    bad_refund = df_orders[df_orders["refund_amount"] > df_orders["paid_amount"]]

    return {
        "bad_amount_cnt": len(bad_amount),
        "bad_refund_cnt": len(bad_refund),
        "amount_diff_sample": bad_amount.head(5)[["order_id", "gross_amount", "item_sum", "diff"]],
        "refund_bad_sample": bad_refund.head(5)[["order_id", "paid_amount", "refund_amount", "status"]],
    }


def main():
    cfg = GenConfig(seed=42, order_cnt=10000, sku_cnt=2000, user_cnt=5000, shop_cnt=200)
    rnd = random.Random(cfg.seed)

    df_user = gen_user_dim(cfg, rnd)
    df_sku = gen_sku_dim(cfg, rnd)
    df_shop = gen_shop_dim(cfg, rnd)

    df_orders, df_items = gen_orders_and_items(cfg, df_sku, df_shop, rnd)

    report = check_consistency(df_orders, df_items)
    print(report["bad_amount_cnt"], report["bad_refund_cnt"])
    print(report["amount_diff_sample"])

