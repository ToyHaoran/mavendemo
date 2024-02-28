DROP TABLE IF EXISTS dws_trade_user_order_td;
CREATE EXTERNAL TABLE dws_trade_user_order_td
(
    `user_id`                   STRING COMMENT '用户ID',
    `order_date_first`          STRING COMMENT '历史至今首次下单日期',
    `order_date_last`           STRING COMMENT '历史至今末次下单日期',
    `order_count_td`            BIGINT COMMENT '历史至今下单次数',
    `order_num_td`              BIGINT COMMENT '历史至今购买商品件数',
    `original_amount_td`        DECIMAL(16, 2) COMMENT '历史至今下单原始金额',
    `activity_reduce_amount_td` DECIMAL(16, 2) COMMENT '历史至今下单活动优惠金额',
    `coupon_reduce_amount_td`   DECIMAL(16, 2) COMMENT '历史至今下单优惠券优惠金额',
    `total_amount_td`           DECIMAL(16, 2) COMMENT '历史至今下单最终金额'
) COMMENT '交易域用户粒度订单历史至今汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');

DROP TABLE IF EXISTS dws_user_user_login_td;
CREATE EXTERNAL TABLE dws_user_user_login_td
(
    `user_id`          STRING COMMENT '用户ID',
    `login_date_last`  STRING COMMENT '历史至今末次登录日期',
    `login_date_first` STRING COMMENT '历史至今首次登录日期',
    `login_count_td`   BIGINT COMMENT '历史至今累计登录次数'
) COMMENT '用户域用户粒度登录历史至今汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_user_user_login_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');
