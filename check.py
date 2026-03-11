def check_head_item_consistency(orders, items, sample_n=200):
    """
    校验订单头和明细是否一致

    校验内容：
    1. total_qty = sum(item_qty)
    2. total_amount = sum(item_amount)

    只抽样检查，避免大数据量卡死
    """

    if not orders or not items:
        return

    # 取前 sample_n 个订单
    sample_orders = orders[:sample_n]

    # 构建 order -> items 映射
    order_items_map = {}

    for it in items:
        oid = it["order_id"]

        if oid not in order_items_map:
            order_items_map[oid] = []

        order_items_map[oid].append(it)

    errors = []

    for o in sample_orders:

        oid = o["order_id"]

        its = order_items_map.get(oid, [])

        qty_sum = sum(i["item_qty"] for i in its)
        amt_sum = sum(i["item_amount"] for i in its)

        if qty_sum != o["total_qty"]:
            errors.append(
                f"order {oid} qty mismatch "
                f"{qty_sum} != {o['total_qty']}"
            )

        if amt_sum != o["total_amount"]:
            errors.append(
                f"order {oid} amount mismatch "
                f"{amt_sum} != {o['total_amount']}"
            )

    if errors:
        print("CHECK FAILED:")
        for e in errors[:10]:
            print(e)
        raise ValueError("order head/item consistency check failed")

    print(f"[check] head-item consistency OK (sample {len(sample_orders)})")