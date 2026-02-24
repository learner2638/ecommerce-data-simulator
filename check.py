# check.py
def check_head_item_consistency(orders, items, sample_n=200):
    order_map = {o["order_id"]: o for o in orders}

    # 1) 头明细 user/shop 一致
    for it in items[:sample_n]:
        o = order_map[it["order_id"]]
        assert it["user_id"] == o["user_id"]
        assert it["shop_id"] == o["shop_id"]

    # 2) 金额/数量一致：抽样订单汇总核对
    sample_order_ids = set(it["order_id"] for it in items[:sample_n])

    item_sum = {}
    for it in items:
        oid = it["order_id"]
        if oid not in sample_order_ids:
            continue
        s = item_sum.setdefault(oid, {"qty": 0, "amt": 0})
        s["qty"] += it["item_qty"]
        s["amt"] += it["item_amount"]

    for oid in sample_order_ids:
        o = order_map[oid]
        s = item_sum.get(oid, {"qty": 0, "amt": 0})
        assert o["total_qty"] == s["qty"]
        assert o["total_amount"] == s["amt"]

    # 3) 状态-时间-金额一致（抽样 orders）
    for o in orders[:min(len(orders), sample_n)]:
        st = o["status"]

        created = o["created_time"]
        pay = o["pay_time"]
        cancel = o["cancel_time"]
        ship = o["ship_time"]
        comp = o["complete_time"]
        rtime = o["refund_time"]

        total = o["total_amount"]
        disc = o["discount_amount"]
        paid = o["paid_amount"]
        refund = o["refund_amount"]

        # 金额边界
        assert 0 <= disc <= total
        assert 0 <= paid <= total
        assert 0 <= refund <= paid

        # 时间单调性
        if pay is not None:
            assert pay >= created
        if cancel is not None:
            assert cancel >= created
        if ship is not None:
            assert pay is not None
            assert ship >= pay
        if comp is not None:
            assert ship is not None
            assert comp >= ship
        if rtime is not None:
            assert pay is not None
            assert rtime >= pay

        # 状态约束
        if st == "UNPAID":
            assert pay is None and cancel is None and ship is None and comp is None and rtime is None
            assert paid == 0 and refund == 0

        elif st == "CANCELLED":
            assert pay is None and ship is None and comp is None and rtime is None
            assert cancel is not None
            assert paid == 0 and refund == 0

        elif st == "PAID":
            assert pay is not None
            assert ship is None and comp is None
            assert cancel is None
            assert paid == total - disc

        elif st == "SHIPPED":
            assert pay is not None and ship is not None
            assert comp is None
            assert cancel is None
            assert paid == total - disc

        elif st == "COMPLETED":
            assert pay is not None and ship is not None and comp is not None
            assert cancel is None
            assert paid == total - disc

        else:
            raise ValueError(f"unknown status: {st}")