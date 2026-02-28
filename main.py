# main.py
from service import run_once

def main():
    result = run_once(
        mode="dev",
        overrides={
            # ✅ 这里想改啥改啥（单个/多个都行）
            "order_cnt": 20000,
            "sku_cnt": 8000,
            "p_refund_given_paid": 0.2,
        },
        do_export=True
    )
    print(result)

if __name__ == "__main__":
    main()