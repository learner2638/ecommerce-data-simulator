from datetime import timedelta


def gen_user_dim(cfg, rnd):
    """
    生成用户维表
    字段：
    - user_id
    - register_time
    - city
    """
    users = []

    for user_id in range(1, cfg.user_cnt + 1):
        days_ago = rnd.randint(cfg.register_days_back_min, cfg.register_days_back_max)
        minute_offset = rnd.randint(0, 24 * 60 - 1)

        register_time = cfg.base_time - timedelta(days=days_ago, minutes=minute_offset)
        city = rnd.choice(cfg.city_pool)

        users.append({
            "user_id": user_id,
            "register_time": register_time,
            "city": city,
        })

    return users


def gen_shop_dim(cfg, rnd):
    """
    生成店铺维表
    字段：
    - shop_id
    - shop_type
    - shop_weight
    """
    shops = []

    shop_types = list(cfg.shop_type_weights.keys())
    shop_type_weights = list(cfg.shop_type_weights.values())

    for shop_id in range(1, cfg.shop_cnt + 1):
        shop_type = rnd.choices(shop_types, weights=shop_type_weights, k=1)[0]

        # 用整数权重即可，后面 facts.py 会做幂次抽样
        shop_weight = rnd.randint(cfg.shop_weight_min, cfg.shop_weight_max)

        shops.append({
            "shop_id": shop_id,
            "shop_type": shop_type,
            "shop_weight": shop_weight,
        })

    return shops


def gen_sku_dim(cfg, rnd):
    """
    生成SKU维表
    字段：
    - sku_id
    - category
    - sku_price

    这里不再依赖 category_price_range，
    统一用全局 sku_price_min / sku_price_max。
    """
    skus = []

    categories = list(cfg.category_buy_weights.keys())
    category_weights = list(cfg.category_buy_weights.values())

    for sku_id in range(1, cfg.sku_cnt + 1):
        category = rnd.choices(categories, weights=category_weights, k=1)[0]
        sku_price = rnd.randint(cfg.sku_price_min, cfg.sku_price_max)

        skus.append({
            "sku_id": sku_id,
            "category": category,
            "sku_price": sku_price,
        })

    return skus