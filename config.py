from datetime import datetime


class Config:
    def __init__(self, mode="dev", **overrides):
        self.mode = mode

        # =========================
        # 1) 基础控制
        # =========================
        self.seed = 42
        self.base_time = datetime(2025, 1, 1, 12, 0, 0)
        self.days_back = 180

        # =========================
        # 2) 数据规模（dev / prod）
        # =========================
        if mode == "dev":
            self.user_cnt = 5_000
            self.shop_cnt = 200
            self.sku_cnt = 2_000
            self.order_cnt = 20_000
            self.batch_size = 5_000
        elif mode == "prod":
            self.user_cnt = 500_000
            self.shop_cnt = 10_000
            self.sku_cnt = 100_000
            self.order_cnt = 5_000_000
            self.batch_size = 50_000
        else:
            raise ValueError(f"unknown mode: {mode}")

        # =========================
        # 3) 类目配置
        # =========================
        self.category_buy_weights = {
            "electronics": 0.20,
            "clothes": 0.25,
            "food": 0.20,
            "home": 0.15,
            "beauty": 0.10,
            "sports": 0.10,
        }

        # =========================
        # 4) 订单结构
        # =========================
        self.runlen_min = 1
        self.runlen_max = 4

        self.qty_min = 1
        self.qty_max = 3

        # =========================
        # 5) 订单状态概率
        # =========================
        self.p_unpaid = 0.08
        self.p_paid = 0.82

        # =========================
        # 6) 履约概率
        # =========================
        self.p_ship_given_paid = 0.92
        self.p_complete_given_shipped = 0.96

        # =========================
        # 7) 退款概率
        # =========================
        self.p_refund_given_paid = 0.12
        self.p_refund_full = 0.40

        self.p_refund_stage_after_pay = 0.50
        self.p_refund_stage_after_ship = 0.35

        # =========================
        # 8) 金额配置
        # =========================
        self.discount_rate_min = 0.02
        self.discount_rate_max = 0.25

        self.refund_rate_min = 0.20
        self.refund_rate_max = 0.80

        # =========================
        # 9) 时间延迟（分钟）
        # =========================
        self.pay_delay_min_min = 1
        self.pay_delay_max_min = 60

        self.cancel_delay_min_min = 5
        self.cancel_delay_max_min = 180

        self.ship_delay_min_min = 30
        self.ship_delay_max_min = 2 * 24 * 60

        self.complete_delay_min_min = 1 * 24 * 60
        self.complete_delay_max_min = 15 * 24 * 60

        self.refund_delay_min_min = 10
        self.refund_delay_max_min = 7 * 24 * 60

        # =========================
        # 10) SKU价格范围（给 dims.py 用）
        # =========================
        self.sku_price_min = 10
        self.sku_price_max = 2000

        # =========================
        # 11) 店铺类型分布（给 dims.py 用）
        # =========================
        self.shop_type_weights = {
            "旗舰店": 0.15,
            "专卖店": 0.25,
            "普通店": 0.45,
            "个体店": 0.15,
        }

        # =========================
        # 12) 城市池（给 dims.py 用）
        # =========================
        self.city_pool = [
            "北京", "上海", "广州", "深圳",
            "杭州", "成都", "武汉", "南京",
            "苏州", "重庆", "西安", "郑州",
            "长沙", "天津", "青岛", "宁波",
        ]

        # =========================
        # 13) 用户注册时间范围（给 dims.py 用）
        # =========================
        self.register_days_back_min = 30
        self.register_days_back_max = 1500

        # =========================
        # 14) 店铺权重范围（给 dims.py 用）
        # =========================
        self.shop_weight_min = 1
        self.shop_weight_max = 100

        # =========================
        # 15) 参数覆盖
        # =========================
        for k, v in overrides.items():
            setattr(self, k, v)

        # =========================
        # 16) 简单校验
        # =========================
        self._validate()

    def _validate(self):
        if self.user_cnt <= 0:
            raise ValueError("user_cnt must > 0")
        if self.shop_cnt <= 0:
            raise ValueError("shop_cnt must > 0")
        if self.sku_cnt <= 0:
            raise ValueError("sku_cnt must > 0")
        if self.order_cnt <= 0:
            raise ValueError("order_cnt must > 0")
        if self.batch_size <= 0:
            raise ValueError("batch_size must > 0")

        if self.runlen_min <= 0 or self.runlen_max < self.runlen_min:
            raise ValueError("runlen range invalid")
        if self.qty_min <= 0 or self.qty_max < self.qty_min:
            raise ValueError("qty range invalid")

        if not (0 <= self.p_unpaid <= 1):
            raise ValueError("p_unpaid must in [0,1]")
        if not (0 <= self.p_paid <= 1):
            raise ValueError("p_paid must in [0,1]")
        if self.p_unpaid + self.p_paid > 1:
            raise ValueError("p_unpaid + p_paid must <= 1")

        if not (0 <= self.p_ship_given_paid <= 1):
            raise ValueError("p_ship_given_paid must in [0,1]")
        if not (0 <= self.p_complete_given_shipped <= 1):
            raise ValueError("p_complete_given_shipped must in [0,1]")

        if not (0 <= self.p_refund_given_paid <= 1):
            raise ValueError("p_refund_given_paid must in [0,1]")
        if not (0 <= self.p_refund_full <= 1):
            raise ValueError("p_refund_full must in [0,1]")

        if self.discount_rate_min < 0 or self.discount_rate_max < self.discount_rate_min:
            raise ValueError("discount rate range invalid")
        if self.refund_rate_min < 0 or self.refund_rate_max < self.refund_rate_min:
            raise ValueError("refund rate range invalid")

        if self.sku_price_min <= 0 or self.sku_price_max < self.sku_price_min:
            raise ValueError("sku price range invalid")

        if not self.category_buy_weights:
            raise ValueError("category_buy_weights cannot be empty")
        if sum(self.category_buy_weights.values()) <= 0:
            raise ValueError("category_buy_weights sum must > 0")

        if not self.shop_type_weights:
            raise ValueError("shop_type_weights cannot be empty")
        if sum(self.shop_type_weights.values()) <= 0:
            raise ValueError("shop_type_weights sum must > 0")

        if not self.city_pool:
            raise ValueError("city_pool cannot be empty")

        if self.register_days_back_min <= 0 or self.register_days_back_max < self.register_days_back_min:
            raise ValueError("register_days_back range invalid")

        if self.shop_weight_min <= 0 or self.shop_weight_max < self.shop_weight_min:
            raise ValueError("shop_weight range invalid")

    def to_dict(self):
        return self.__dict__