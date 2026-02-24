# pipeline.py
from dims import gen_user_dim, gen_shop_dim, gen_sku_dim
from facts import gen_orders, gen_order_items

def build_dataset(cfg, rnd):
    users = gen_user_dim(cfg, rnd)
    shops = gen_shop_dim(cfg, rnd)
    skus  = gen_sku_dim(cfg, rnd)

    orders = gen_orders(cfg, rnd, shops=shops)
    items  = gen_order_items(cfg, rnd, orders=orders, skus=skus)

    return {
        "users": users,
        "shops": shops,
        "skus": skus,
        "orders": orders,
        "items": items,
    }