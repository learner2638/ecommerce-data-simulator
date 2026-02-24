# config.py
from datetime import datetime

class Config:
    def __init__(self, mode="dev"):
        # 可复现
        self.seed = 42
        self.base_time = datetime(2026, 2, 20, 0, 0, 0)

        if mode == "dev":
            self.user_cnt = 1000
            self.shop_cnt = 200
            self.sku_cnt = 2000
            self.order_cnt = 5000
        elif mode == "prod":
            self.user_cnt = 100000
            self.shop_cnt = 10000
            self.sku_cnt = 200000
            self.order_cnt = 1000000
        else:
            raise ValueError("mode must be dev or prod")

        # 时间范围
        self.days_back = 90

        # 订单基础状态概率（第一层：UNPAID / PAID / CANCELLED）
        self.p_unpaid = 0.1
        self.p_paid = 0.8
        self.p_cancel = 0.1

        # 每单明细行数范围
        self.runlen_min = 1
        self.runlen_max = 8

        # 每行购买数量
        self.qty_min = 1
        self.qty_max = 3

        # ===== 金额/时间体系 =====
        # 支付延迟（分钟）
        self.pay_delay_min_min = 1
        self.pay_delay_max_min = 24 * 60

        # 未支付取消延迟（分钟）
        self.cancel_delay_min_min = 5
        self.cancel_delay_max_min = 3 * 24 * 60

        # 折扣率（对 total_amount）
        self.discount_rate_min = 0.00
        self.discount_rate_max = 0.30

        # 已支付订单退款概率
        self.p_refund_given_paid = 0.08

        # 退款延迟（分钟）
        self.refund_delay_min_min = 30
        self.refund_delay_max_min = 15 * 24 * 60

        # 部分退款比例区间（对 paid_amount）
        self.refund_rate_min = 0.10
        self.refund_rate_max = 1.00

        # ===== 履约链路（PAID -> SHIPPED -> COMPLETED）=====
        self.p_ship_given_paid = 0.95
        self.p_complete_given_shipped = 0.93

        # 支付到发货间隔（分钟）
        self.ship_delay_min_min = 30
        self.ship_delay_max_min = 5 * 24 * 60

        # 发货到完成间隔（分钟）
        self.complete_delay_min_min = 6 * 60
        self.complete_delay_max_min = 15 * 24 * 60

        # ===== 退款细化 =====
        self.p_refund_full = 0.35
        self.p_refund_stage_after_pay = 0.45
        self.p_refund_stage_after_ship = 0.45
        self.p_refund_stage_after_complete = 0.10

        # ===== 类目控价（更真实价格分布）=====
        self.category_price_range = {
            "electronics": (300, 5000),
            "clothes": (50, 800),
            "food": (5, 200),
            "home": (30, 2000),
            "beauty": (20, 1500),
        }

        # ===== 类目购买权重（更真实购买结构）=====
        self.category_buy_weights = {
            "electronics": 1.2,
            "clothes": 2.2,
            "food": 3.5,
            "home": 1.6,
            "beauty": 1.8,
        }