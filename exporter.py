# exporter.py
import csv
import os
import shutil
from datetime import datetime


# ========= 工具函数 =========
def _ensure_dir(path: str):
    if path:
        os.makedirs(path, exist_ok=True)


def _fmt_value(v):
    """把 datetime / None 等格式化成 CSV 可写的字符串"""
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return v


def export_csv(rows, filepath, fieldnames):
    """
    rows: list[dict]
    filepath: 输出路径
    fieldnames: 列顺序（只导出这些列）
    """
    _ensure_dir(os.path.dirname(filepath))
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            out = {k: _fmt_value(r.get(k)) for k in fieldnames}
            writer.writerow(out)


# ========= ODS 导出 =========
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


def export_ods(ds, out_dir="out/ods"):
    """
    按你 pipeline 产出的 ds 结构导出 ODS CSV：
    ds = {users, shops, skus, orders, items}
    """
    _ensure_dir(out_dir)

    export_csv(ds["users"], os.path.join(out_dir, "ods_user_dim.csv"), ODS_SCHEMA["ods_user_dim"])
    export_csv(ds["shops"], os.path.join(out_dir, "ods_shop_dim.csv"), ODS_SCHEMA["ods_shop_dim"])
    export_csv(ds["skus"], os.path.join(out_dir, "ods_sku_dim.csv"), ODS_SCHEMA["ods_sku_dim"])
    export_csv(ds["orders"], os.path.join(out_dir, "ods_orders.csv"), ODS_SCHEMA["ods_orders"])
    export_csv(ds["items"], os.path.join(out_dir, "ods_order_items.csv"), ODS_SCHEMA["ods_order_items"])

    return out_dir


def pack_ods_zip(ods_dir: str, zip_path: str | None = None):
    """
    把 ods_dir 打成 zip。
    - ods_dir: out/ods
    - zip_path: out/ods.zip（可不传，默认 ods_dir 同级 ods.zip）
    """
    ods_dir = os.path.abspath(ods_dir)
    if zip_path is None:
        zip_path = os.path.join(os.path.dirname(ods_dir), "ods.zip")

    base_name = os.path.splitext(os.path.abspath(zip_path))[0]  # shutil 需要不带 .zip 的 base_name
    _ensure_dir(os.path.dirname(base_name))

    zip_file = shutil.make_archive(base_name=base_name, format="zip", root_dir=ods_dir)
    return zip_file


# ========= Hive ODS DDL =========
def hive_ddl_ods(database="dw_ods"):
    """
    生成 Hive ODS 建表 DDL（与 CSV 列一一对应）
    注意：这里是最通用的 TEXTFILE + 逗号分隔，适配你导出的 CSV。
    """
    ddls = []

    ddls.append(f"""-- ODS: database
CREATE DATABASE IF NOT EXISTS {database};
""")

    ddls.append(f"""-- ODS: 用户维表
CREATE TABLE IF NOT EXISTS {database}.ods_user_dim (
  user_id        INT,
  register_time  STRING,
  city           STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
""")

    ddls.append(f"""-- ODS: 店铺维表
CREATE TABLE IF NOT EXISTS {database}.ods_shop_dim (
  shop_id      INT,
  shop_type    STRING,
  shop_weight  DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
""")

    ddls.append(f"""-- ODS: SKU 维表
CREATE TABLE IF NOT EXISTS {database}.ods_sku_dim (
  sku_id     INT,
  category   STRING,
  sku_price  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
""")

    ddls.append(f"""-- ODS: 订单头
CREATE TABLE IF NOT EXISTS {database}.ods_orders (
  order_id         INT,
  user_id          INT,
  shop_id          INT,
  created_time     STRING,
  status           STRING,

  total_qty        INT,
  total_amount     INT,

  discount_amount  INT,
  paid_amount      INT,
  refund_amount    INT,

  pay_time         STRING,
  cancel_time      STRING,
  ship_time        STRING,
  complete_time    STRING,
  refund_time      STRING,

  refund_type      STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
""")

    ddls.append(f"""-- ODS: 订单明细
CREATE TABLE IF NOT EXISTS {database}.ods_order_items (
  order_item_id  INT,
  order_id       INT,
  user_id        INT,
  shop_id        INT,
  sku_id         INT,
  item_qty       INT,
  sku_price      INT,
  item_amount    INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
""")

    return "\n".join(ddls)


def write_hive_ddl(filepath="out/hive_ods_ddl.sql", database="dw_ods"):
    _ensure_dir(os.path.dirname(filepath))
    ddl = hive_ddl_ods(database=database)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(ddl)
    return filepath