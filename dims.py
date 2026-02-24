# dim.py
from datetime import timedelta

def gen_user_dim(cfg, rnd):
    users = []
    now = cfg.base_time
    cities = ["beijing", "shanghai", "guangzhou", "shenzhen"]

    for uid in range(1, cfg.user_cnt + 1):
        users.append({
            "user_id": uid,
            "register_time": now - timedelta(days=rnd.randint(1, 1200)),
            "city": rnd.choice(cities),
        })
    return users


def gen_shop_dim(cfg, rnd):
    shops = []
    for sid in range(1, cfg.shop_cnt + 1):
        shop_type = rnd.choices(
            ["S", "M", "L"],
            weights=[0.7, 0.25, 0.05],
            k=1
        )[0]

        base_weight = {"S": 1.0, "M": 3.0, "L": 8.0}[shop_type]
        shop_weight = base_weight * (0.7 + rnd.random() * 0.6)

        shops.append({
            "shop_id": sid,
            "shop_type": shop_type,
            "shop_weight": round(shop_weight, 4),
        })
    return shops


def gen_sku_dim(cfg, rnd):
    """
    ✅ 类目控价：不同类目不同价格区间
    """
    skus = []
    categories = list(cfg.category_price_range.keys())

    for sku_id in range(1, cfg.sku_cnt + 1):
        cat = rnd.choice(categories)
        lo, hi = cfg.category_price_range[cat]
        price = rnd.randint(lo, hi)

        skus.append({
            "sku_id": sku_id,
            "category": cat,
            "sku_price": price
        })

    return skus