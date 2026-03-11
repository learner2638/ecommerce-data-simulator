import os
import time

ORDERS_PATH = r"out/ods/ods_orders.csv"
ITEMS_PATH = r"out/ods/ods_order_items.csv"

# 你这次任务目标
TARGET_ORDERS = 5_000_000
EST_ITEMS_PER_ORDER = 2.5
TARGET_ITEMS = int(TARGET_ORDERS * EST_ITEMS_PER_ORDER)

# 经验值：先按你之前 100w 测试粗估单行平均字节数
# 后面你也可以自己改得更准
ORDER_BYTES_PER_ROW = 150
ITEM_BYTES_PER_ROW = 90


def file_size_mb(path: str) -> float:
    if not os.path.exists(path):
        return 0.0
    return os.path.getsize(path) / 1024 / 1024


def estimate_rows(path: str, bytes_per_row: int) -> int:
    if not os.path.exists(path):
        return 0
    size = os.path.getsize(path)
    return max(0, int(size / bytes_per_row) - 1)  # 粗略减掉表头


def fmt_seconds(sec: float) -> str:
    if sec <= 0:
        return "--"
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def main():
    print("Start monitoring CSV progress...\n")

    last_time = time.time()
    last_orders = 0
    last_items = 0

    while True:
        now = time.time()
        dt = max(now - last_time, 1e-6)

        orders_mb = file_size_mb(ORDERS_PATH)
        items_mb = file_size_mb(ITEMS_PATH)

        est_orders = estimate_rows(ORDERS_PATH, ORDER_BYTES_PER_ROW)
        est_items = estimate_rows(ITEMS_PATH, ITEM_BYTES_PER_ROW)

        d_orders = est_orders - last_orders
        d_items = est_items - last_items

        order_speed = d_orders / dt
        item_speed = d_items / dt

        order_progress = min(est_orders / TARGET_ORDERS, 1.0) if TARGET_ORDERS else 0
        item_progress = min(est_items / TARGET_ITEMS, 1.0) if TARGET_ITEMS else 0

        remain_orders = max(TARGET_ORDERS - est_orders, 0)
        remain_items = max(TARGET_ITEMS - est_items, 0)

        eta_orders = remain_orders / order_speed if order_speed > 0 else -1
        eta_items = remain_items / item_speed if item_speed > 0 else -1

        os.system("cls" if os.name == "nt" else "clear")
        print("=== ODS CSV Monitor ===")
        print(f"orders file size      : {orders_mb:.2f} MB")
        print(f"order_items file size : {items_mb:.2f} MB")
        print()
        print(f"estimated orders      : {est_orders:,} / {TARGET_ORDERS:,} ({order_progress:.1%})")
        print(f"estimated items       : {est_items:,} / {TARGET_ITEMS:,} ({item_progress:.1%})")
        print()
        print(f"order speed           : {order_speed:,.0f} rows/s")
        print(f"item speed            : {item_speed:,.0f} rows/s")
        print()
        print(f"ETA orders            : {fmt_seconds(eta_orders)}")
        print(f"ETA items             : {fmt_seconds(eta_items)}")
        print()
        print("Press Ctrl+C to stop monitoring.")

        last_time = now
        last_orders = est_orders
        last_items = est_items

        time.sleep(2)


if __name__ == "__main__":
    main()