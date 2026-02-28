# config.py
from datetime import datetime

# 1) dev/prod 只是预设（默认值集合）
PRESETS = {
    "dev": dict(
        user_cnt=1000,
        shop_cnt=200,
        sku_cnt=2000,
        order_cnt=5000,
    ),
    "prod": dict(
        user_cnt=100000,
        shop_cnt=10000,
        sku_cnt=200000,
        order_cnt=1000000,
    ),
}

class Config:
    def __init__(self, mode="dev", **overrides):
        if mode not in PRESETS:
            raise ValueError("mode must be dev or prod")

        # ===== 基础字段 =====
        self.mode = mode
        self.seed = 42
        self.base_time = datetime(2026, 2, 20, 0, 0, 0)

        # ===== 先加载 preset 默认值（dev/prod）=====
        for k, v in PRESETS[mode].items():
            setattr(self, k, v)

        # ===== 你的全量业务参数默认值（全部都可以被覆盖）=====
        self.days_back = 90

        self.p_unpaid = 0.1
        self.p_paid = 0.8
        self.p_cancel = 0.1

        self.runlen_min = 1
        self.runlen_max = 8

        self.qty_min = 1
        self.qty_max = 3

        self.pay_delay_min_min = 1
        self.pay_delay_max_min = 24 * 60

        self.cancel_delay_min_min = 5
        self.cancel_delay_max_min = 3 * 24 * 60

        self.discount_rate_min = 0.00
        self.discount_rate_max = 0.30

        self.p_refund_given_paid = 0.08

        self.refund_delay_min_min = 30
        self.refund_delay_max_min = 15 * 24 * 60

        self.refund_rate_min = 0.10
        self.refund_rate_max = 1.00

        self.p_ship_given_paid = 0.95
        self.p_complete_given_shipped = 0.93

        self.ship_delay_min_min = 30
        self.ship_delay_max_min = 5 * 24 * 60

        self.complete_delay_min_min = 6 * 60
        self.complete_delay_max_min = 15 * 24 * 60

        self.p_refund_full = 0.35
        self.p_refund_stage_after_pay = 0.45
        self.p_refund_stage_after_ship = 0.45
        self.p_refund_stage_after_complete = 0.10

        self.category_price_range = {
            "electronics": (300, 5000),
            "clothes": (50, 800),
            "food": (5, 200),
            "home": (30, 2000),
            "beauty": (20, 1500),
        }

        self.category_buy_weights = {
            "electronics": 1.2,
            "clothes": 2.2,
            "food": 3.5,
            "home": 1.6,
            "beauty": 1.8,
        }

        # ===== 最后：覆盖任何字段（核心）=====
        self.apply_overrides(overrides)

    def apply_overrides(self, overrides: dict | None):
        if not overrides:
            return

        for k, v in overrides.items():
            if not hasattr(self, k):
                # 不认识的字段直接报错（防止拼错参数名导致“改了个寂寞”）
                raise ValueError(f"Unknown config field: {k}")

            old = getattr(self, k)

            # 自动类型转换：传进来是字符串也能变成 int/float/bool
            if v is None:
                continue
            if isinstance(old, bool):
                if isinstance(v, str):
                    v = v.lower() in ("1", "true", "yes", "y", "t")
                else:
                    v = bool(v)
            elif isinstance(old, int) and not isinstance(old, bool):
                v = int(v)
            elif isinstance(old, float):
                v = float(v)

            setattr(self, k, v)

    def to_dict(self):
        d = dict(self.__dict__)
        if isinstance(d.get("base_time"), datetime):
            d["base_time"] = d["base_time"].isoformat()
        return d