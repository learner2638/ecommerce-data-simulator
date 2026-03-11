import time
from service import run_once_stream_parallel

def main():
    t0 = time.time()

    result = run_once_stream_parallel(
        mode="prod",
        overrides={
            "order_cnt": 10_000_000,
            "user_cnt": 120_000,
            "shop_cnt": 8_000,
            "sku_cnt": 35_000,
            "p_refund_given_paid": 0.18,
        },
        batch_size=300_000,
        workers=6,
        do_export=True,
        keep_parts=False,
    )

    t1 = time.time()
    print("done")
    print(f"elapsed={t1 - t0:.2f}s")
    print(result)

if __name__ == "__main__":
    main()