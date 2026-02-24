# main.py
import random

from config import Config
from pipeline import build_dataset
from check import check_head_item_consistency


def main():
    cfg = Config(mode="dev")
    rnd = random.Random(cfg.seed)

    ds = build_dataset(cfg, rnd)

    users = ds["users"]
    shops = ds["shops"]
    skus = ds["skus"]
    orders = ds["orders"]
    items = ds["items"]

    print("users 行数:", len(users))
    print("shops 行数:", len(shops))
    print("skus 行数:", len(skus))
    print("orders 行数:", len(orders))
    print("order_item 行数:", len(items))

    print("\nshops 前2条:")
    for s in shops[:2]:
        print(s)

    print("\nskus 前2条:")
    for s in skus[:2]:
        print(s)

    print("\norders 前2条:")
    for o in orders[:2]:
        print(o)

    print("\norder_item 前10条:")
    for it in items[:10]:
        print(it)

    # ✅ 校验：头明细一致 + 金额数量一致 +（你升级后 check 里还会校验状态/时间）
    check_head_item_consistency(orders, items, sample_n=200)
    print("\n校验通过：头明细一致 + 金额数量一致 + 状态时间金额一致")

    # ===== 统计分析：状态分布 + 退款率 =====
    cnt = {}
    refund_paid = 0

    for o in orders:
        st = o["status"]
        cnt[st] = cnt.get(st, 0) + 1

        # “发生退款”的定义：已支付且 refund_amount > 0
        if o.get("paid_amount", 0) > 0 and o.get("refund_amount", 0) > 0:
            refund_paid += 1

    print("\n状态分布:", cnt)

    # 已支付订单：PAID + SHIPPED + COMPLETED（如果你后面再加新状态，这里也可以继续扩展）
    paid_cnt = (
        cnt.get("PAID", 0)
        + cnt.get("SHIPPED", 0)
        + cnt.get("COMPLETED", 0)
    )

    print("已支付占比(含发货/完成):", round(paid_cnt / len(orders), 4))
    print("退款率(在已支付订单内):", round(refund_paid / max(paid_cnt, 1), 4))
    from collections import Counter

    user_cnt = Counter(o["user_id"] for o in orders)
    top10 = user_cnt.most_common(10)
    print("\n下单最多的 Top10 用户:", top10)
    print("Top10 订单占比:", round(sum(c for _, c in top10) / len(orders), 4))
    from collections import Counter

    # Top用户
    uc = Counter(o["user_id"] for o in orders)
    top10u = uc.most_common(10)
    print("\nTop10 用户:", top10u)
    print("Top10 用户订单占比:", round(sum(c for _, c in top10u) / len(orders), 4))

    # Top店铺
    sc = Counter(o["shop_id"] for o in orders)
    top10s = sc.most_common(10)
    print("\nTop10 店铺:", top10s)
    print("Top10 店铺订单占比:", round(sum(c for _, c in top10s) / len(orders), 4))

    # Top SKU（爆品）
    kc = Counter(it["sku_id"] for it in items)
    top10k = kc.most_common(10)
    print("\nTop10 SKU:", top10k)
    print("Top10 SKU 明细占比:", round(sum(c for _, c in top10k) / len(items), 4))
    from exporter import export_ods, write_hive_ddl, pack_ods_zip

    ods_dir = export_ods(ds, out_dir="out/ods")
    ddl_path = write_hive_ddl("out/hive_ods_ddl.sql", database="dw_ods")
    zip_path = pack_ods_zip(ods_dir)  # out/ods.zip
    print("已导出:", ods_dir, ddl_path, zip_path)
if __name__ == "__main__":
    main()