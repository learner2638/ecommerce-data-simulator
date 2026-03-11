import os
import shutil
from datetime import datetime


# ========= 参数 =========
DEFAULT_BUFFER_SIZE = 50000
DEFAULT_FILE_BUFFERING = 16 * 1024 * 1024


# ========= 工具函数 =========
def _ensure_dir(path: str):
    if path:
        os.makedirs(path, exist_ok=True)


def _fmt(v):
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return str(v)


def _row_to_csv_line(row, fieldnames):
    """
    把 dict 转成 CSV 行字符串
    """
    return ",".join(_fmt(row.get(k)) for k in fieldnames)


# ========= CSV 写入 =========
def export_csv(rows, filepath, fieldnames, buffer_size=DEFAULT_BUFFER_SIZE):
    """
    一次性导出 CSV（维表）
    """
    _ensure_dir(os.path.dirname(filepath))

    with open(
        filepath,
        "w",
        encoding="utf-8",
        buffering=DEFAULT_FILE_BUFFERING,
        newline=""
    ) as f:

        # header
        f.write(",".join(fieldnames) + "\n")

        buf = []

        for r in rows:
            buf.append(_row_to_csv_line(r, fieldnames))

            if len(buf) >= buffer_size:
                f.write("\n".join(buf) + "\n")
                buf.clear()

        if buf:
            f.write("\n".join(buf) + "\n")


def init_csv(filepath, fieldnames):
    """
    初始化 CSV
    """
    _ensure_dir(os.path.dirname(filepath))

    with open(
        filepath,
        "w",
        encoding="utf-8",
        buffering=DEFAULT_FILE_BUFFERING,
        newline=""
    ) as f:
        f.write(",".join(fieldnames) + "\n")


def append_csv(rows, filepath, fieldnames, buffer_size=DEFAULT_BUFFER_SIZE):
    """
    高速追加 CSV
    """
    if not rows:
        return

    _ensure_dir(os.path.dirname(filepath))

    with open(
        filepath,
        "a",
        encoding="utf-8",
        buffering=DEFAULT_FILE_BUFFERING,
        newline=""
    ) as f:

        buf = []

        for r in rows:
            buf.append(_row_to_csv_line(r, fieldnames))

            if len(buf) >= buffer_size:
                f.write("\n".join(buf) + "\n")
                buf.clear()

        if buf:
            f.write("\n".join(buf) + "\n")


# ========= ODS schema =========
ODS_SCHEMA = {
    "ods_user_dim": ["user_id", "register_time", "city"],
    "ods_shop_dim": ["shop_id", "shop_type", "shop_weight"],
    "ods_sku_dim": ["sku_id", "category", "sku_price"],
    "ods_orders": [
        "order_id", "user_id", "shop_id", "created_time", "status",
        "total_qty", "total_amount",
        "discount_amount", "paid_amount", "refund_amount",
        "pay_time", "cancel_time", "ship_time", "complete_time", "refund_time",
        "refund_type",
    ],
    "ods_order_items": [
        "order_item_id", "order_id", "user_id", "shop_id", "sku_id",
        "item_qty", "sku_price", "item_amount",
    ],
}


# ========= ODS 导出 =========
def export_ods(ds, out_dir="out/ods"):
    _ensure_dir(out_dir)

    export_csv(ds["users"], os.path.join(out_dir, "ods_user_dim.csv"), ODS_SCHEMA["ods_user_dim"])
    export_csv(ds["shops"], os.path.join(out_dir, "ods_shop_dim.csv"), ODS_SCHEMA["ods_shop_dim"])
    export_csv(ds["skus"], os.path.join(out_dir, "ods_sku_dim.csv"), ODS_SCHEMA["ods_sku_dim"])
    export_csv(ds["orders"], os.path.join(out_dir, "ods_orders.csv"), ODS_SCHEMA["ods_orders"])
    export_csv(ds["items"], os.path.join(out_dir, "ods_order_items.csv"), ODS_SCHEMA["ods_order_items"])

    return out_dir


# ========= 打包 =========
def pack_ods_zip(ods_dir: str, zip_path: str | None = None):
    ods_dir = os.path.abspath(ods_dir)

    if zip_path is None:
        zip_path = os.path.join(os.path.dirname(ods_dir), "ods.zip")

    base_name = os.path.splitext(os.path.abspath(zip_path))[0]

    _ensure_dir(os.path.dirname(base_name))

    return shutil.make_archive(
        base_name=base_name,
        format="zip",
        root_dir=ods_dir
    )


# ========= Hive DDL =========
def hive_ddl_ods(database="dw_ods"):
    return f"""
CREATE DATABASE IF NOT EXISTS {database};

CREATE TABLE IF NOT EXISTS {database}.ods_user_dim (
  user_id INT,
  register_time STRING,
  city STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS {database}.ods_shop_dim (
  shop_id INT,
  shop_type STRING,
  shop_weight DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS {database}.ods_sku_dim (
  sku_id INT,
  category STRING,
  sku_price INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS {database}.ods_orders (
  order_id INT,
  user_id INT,
  shop_id INT,
  created_time STRING,
  status STRING,
  total_qty INT,
  total_amount INT,
  discount_amount INT,
  paid_amount INT,
  refund_amount INT,
  pay_time STRING,
  cancel_time STRING,
  ship_time STRING,
  complete_time STRING,
  refund_time STRING,
  refund_type STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS {database}.ods_order_items (
  order_item_id BIGINT,
  order_id INT,
  user_id INT,
  shop_id INT,
  sku_id INT,
  item_qty INT,
  sku_price INT,
  item_amount INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
"""


def write_hive_ddl(filepath="out/hive_ods_ddl.sql", database="dw_ods"):
    _ensure_dir(os.path.dirname(filepath))

    with open(
        filepath,
        "w",
        encoding="utf-8",
        buffering=DEFAULT_FILE_BUFFERING
    ) as f:
        f.write(hive_ddl_ods(database))

    return filepath