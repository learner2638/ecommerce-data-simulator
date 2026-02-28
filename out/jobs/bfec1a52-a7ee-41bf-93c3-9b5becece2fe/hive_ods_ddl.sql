-- ODS: database
CREATE DATABASE IF NOT EXISTS dw_ods;

-- ODS: 用户维表
CREATE TABLE IF NOT EXISTS dw_ods.ods_user_dim (
  user_id        INT,
  register_time  STRING,
  city           STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- ODS: 店铺维表
CREATE TABLE IF NOT EXISTS dw_ods.ods_shop_dim (
  shop_id      INT,
  shop_type    STRING,
  shop_weight  DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- ODS: SKU 维表
CREATE TABLE IF NOT EXISTS dw_ods.ods_sku_dim (
  sku_id     INT,
  category   STRING,
  sku_price  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- ODS: 订单头
CREATE TABLE IF NOT EXISTS dw_ods.ods_orders (
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

-- ODS: 订单明细
CREATE TABLE IF NOT EXISTS dw_ods.ods_order_items (
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
