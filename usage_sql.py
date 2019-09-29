"""
Author: yifan@netopstec.com

Create_time: 2018-04-12

Description: 封装建表、获取订单数据的sql，所有品牌仅需传入品牌名等参数

Process:
    1. 从服务器的order表和trade表中获取订单数据
    2. 建表sql，建立清洗后数据的存储表
    3. 建立统计表，buyer_id唯一
    4. 统计sql语句（有2条sql）
    5. 订单表中设立buyer_id字段，由统计表反向填充
    6. 生成统计数据表

update：
    0408： 删除订单表的城市和区域字段
"""


# 1. 从order表和trade表中获取订单数据
def get_order(table_num, visitor_id, s_time, e_time):
    # 一共26个字段
    sql_get_order = """
        SELECT
            a.tid,
            a.created,
            a.pay_time,          
    
            a.buyer_nick,
            a.receiver_mobile,
            a.receiver_state,

            b.outer_iid,
            b.outer_sku_id,
            b.num_iid,
            b.title,
            b.`sku_properties_name`,
            b.num,
            
            b.`status`,
            trade_from,
            a.`type`,
            
            b.price,
            a.total_fee,
            a.post_fee,
            b.payment,
            divide_order_fee
            
        FROM
            tb_trade_{} AS a,
            tb_order_{} AS b
        WHERE
            a.tid = b.tid
        AND a.visitor_id = {}
        and pay_time >= '{}'
        and pay_time < '{}'

    """.format(table_num, table_num, visitor_id, s_time, e_time)
    return sql_get_order


# 2. 建立一张表用于存放清洗后的数据
def create_order_table(table_name, comment):
    # 一共28个字段，26个字段 + 2个id字段
    sql_create_order = """
            drop table if exists {};
            CREATE TABLE `{}` (
    
              `id` int (20) NOT NULL AUTO_INCREMENT COMMENT '索引',
              `buyer_id` INT (20) DEFAULT NULL COMMENT 'buyer_id' ,                              
              `buyer_nick` varchar(100) DEFAULT NULL COMMENT '买家会员名',
              `receiver_mobile` varchar(40) DEFAULT NULL COMMENT '收货人手机号',
              `receiver_state` varchar(20) DEFAULT NULL COMMENT '省份',
              
              `tid` varchar(100) DEFAULT NULL COMMENT '订单ID' ,
              `created` datetime DEFAULT NULL COMMENT '订单创建时间',
              `pay_time` datetime DEFAULT NULL COMMENT '订单支付时间',       
              
              `status` varchar(32) DEFAULT NULL COMMENT '订单状态',
              `trade_from` varchar(100) DEFAULT NULL COMMENT '订单渠道：taobao, WAP(应该是手机端)',
              `type` varchar(16) DEFAULT NULL COMMENT '是否是正式销售：fixed: 正式；step: 预售',
              `outer_iid` varchar(64) DEFAULT NULL COMMENT '商品编码',
              `num_iid` varchar(32) DEFAULT NULL COMMENT '页面id',     
              `outer_sku_id` varchar(64) DEFAULT NULL COMMENT '商品sku_id',               
              `title` varchar(150) DEFAULT NULL COMMENT '商品标题',                    
              `sku_properties_name` varchar(64) NULL COMMENT '订单商品描述',
              `sales`  decimal(10,2) DEFAULT NULL COMMENT '支付金额',            
              `num` int(18) DEFAULT NULL COMMENT '购买数量',          
    
              `price` decimal(18,2) DEFAULT NULL COMMENT '商品原价',
              `total_fee` decimal(10,2) DEFAULT NULL COMMENT '总费用（按原价算）',                  
              `payment` decimal(18,2) DEFAULT NULL COMMENT '原支付金额,(废弃)',
              `divide_order_fee` decimal(10,2) DEFAULT NULL COMMENT '现支付金额,(废弃)',          
              `post_fee` decimal(10,2) DEFAULT NULL COMMENT '邮费',          
    
              PRIMARY KEY (`id`),
              KEY `buyer` (`buyer_id`) USING BTREE,
              key `buyer_nick` (`buyer_nick`),
              KEY `numiid_title` (`num_iid`, `title`),
              KEY `pay_time` (`pay_time`)      
            ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='{}';
        """.format(table_name, table_name, comment)
    return sql_create_order


# 3. 创建会员消费统计表
def create_stat(table_name, comment):
    # 一共17个字段
    sql_create_stat = """
        drop table if exists `{}`;
        CREATE TABLE `{}` (        
          `buyer_id` int (20) NOT NULL AUTO_INCREMENT COMMENT 'buyer_id',
          `buyer_nick` varchar(100) DEFAULT NULL COMMENT '买家会员名',
          `first_purchase_time` datetime DEFAULT NULL COMMENT '第一次购买时间',
          `first_away` int(10) DEFAULT NULL COMMENT '首单距今消费时间',
          `last_purchase_time` datetime DEFAULT NULL COMMENT '最后一次购买时间',
          `last_away` int(10) DEFAULT NULL COMMENT '最后单距今消费时间',      
          `total_payment` decimal(15,2) DEFAULT NULL COMMENT '累计消费金额',
          `times` int(10) DEFAULT NULL COMMENT '累计消费次（天）数',
          `min_payment` decimal(15,2) DEFAULT NULL COMMENT '最小消费金额',
          `max_payment` decimal(15,2) DEFAULT NULL COMMENT '最大消费金额',
          `avg_payment` decimal(15,2) DEFAULT NULL COMMENT '平均客单价',
          PRIMARY KEY (`buyer_id`),
          UNIQUE KEY `buyer` (`buyer_id`),
          UNIQUE key `buyer_nick` (`buyer_nick`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='{}';      
        """.format(table_name, table_name, comment)
    return sql_create_stat


# 3.1 创建会员消费统计表——奶粉专用
def create_stat_milk(table_name, comment):
    # 一共17个字段
    sql = """
        drop table if exists `{}`;
        CREATE TABLE `{}` (        
          `buyer_id` int (20) NOT NULL AUTO_INCREMENT COMMENT 'buyer_id',
          `buyer_nick` varchar(100) DEFAULT NULL COMMENT '买家会员名',
          `first_purchase_time` datetime DEFAULT NULL COMMENT '第一次购买时间',
          `first_purchase_mon` datetime DEFAULT NULL COMMENT '第一次购买月份',
          `first_away` int(10) DEFAULT NULL COMMENT '首单距今消费时间',
          `last_purchase_time` datetime DEFAULT NULL COMMENT '最后一次购买时间',
          `last_away` int(10) DEFAULT NULL COMMENT '最后单距今消费时间',      
          `recent_times1` int(10)  DEFAULT NULL COMMENT '近30天购买次数（不含退拒）',
          `recent_payment1` decimal(15,2) DEFAULT NULL COMMENT '近30天购买金额（不含退拒）' , 
          `recent_times2` int(10)  DEFAULT NULL COMMENT '近30天购买次数（含退拒）',
          `recent_payment2` decimal(15,2)  DEFAULT NULL COMMENT '近30天购买金额（含退拒）', 
          `total_payment` decimal(15,2) DEFAULT NULL COMMENT '累计消费金额',
          `times` int(10) DEFAULT NULL COMMENT '累计消费次（天）数',
          `min_payment` decimal(15,2) DEFAULT NULL COMMENT '最小消费金额',
          `max_payment` decimal(15,2) DEFAULT NULL COMMENT '最大消费金额',
          `avg_payment` decimal(15,2) DEFAULT NULL COMMENT '平均客单价',
          `total_discount_fee` decimal(15,2) DEFAULT NULL COMMENT '累计使用代金券金额',
          `total_discount_times` int(10) DEFAULT NULL COMMENT '累计使用代金券次数',
          PRIMARY KEY (`buyer_id`),
          UNIQUE KEY `buyer` (`buyer_id`),
          UNIQUE key `buyer_nick` (`buyer_nick`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='{}';      
        """.format(table_name, table_name, comment)
    return sql


# 4. 会员消费数据统计sql语句
# 4.1 按双十一当天切割，以计算payment和divide
def order_stat1(table):
    sql_order_stat1 = """
        SELECT
          buyer_nick,
          SUM(payment_) AS total_payment,
          COUNT(the_date) AS times,
          SUM(payment_) /  COUNT(the_date) AS avg_payment,
         
          MIN(payment_) AS min_payment,
          MAX(payment_) AS max_payment,
          
          MAX(pay_time) AS last_purchase_time,
          DATEDIFF(NOW(), MAX(pay_time)) AS last_away,
          
          MIN(pay_time) AS first_purchase_time,
          date_format(MIN(pay_time), '%Y-%m') AS first_purchase_month,
          DATEDIFF(NOW(), MIN(pay_time)) AS first_away
          
        FROM
          (SELECT
            buyer_nick,
            DATE_FORMAT(pay_time, '%Y-%m-%d') AS the_date,
            SUM(sales) AS payment_,
            pay_time
          FROM
            {}
          GROUP BY buyer_nick,
            the_date) AS a
        GROUP BY buyer_nick
        """.format(table)
    return sql_order_stat1


# 4.2 近30天的购买数据
def order_stat2(table):
    sql_order_stat2 = """
        SELECT
            buyer_nick,
            count(1) AS recent_times1,
            sum(payment_) AS recent_payment1
        FROM
            (
                SELECT
                    buyer_nick,
                    DATE_FORMAT(pay_time, '%Y-%m-%d') AS the_date,
                    sum(divide_order_fee) AS payment_,
                    pay_time
                FROM
                    `{}`
                WHERE
                 DATEDIFF(now(), pay_time) <= 30                                     
                GROUP BY
                    buyer_nick,
                    the_date
            ) AS a
        GROUP BY
            buyer_nick""".format(table)
    return sql_order_stat2


# 5. 给订单表插入buyer_id
def update_buyer_id(table_target, table_id):
    sql_update = """
      UPDATE {} as f
        INNER JOIN {} as fs ON f.buyer_nick = fs.buyer_nick
        SET f.buyer_id = fs.buyer_id
        """.format(table_target, table_id)
    return sql_update


# 6. 获取最大订单日期
def get_last_day(table_name):
    sql = """
    select 
        date_add(date_format(max(pay_time), "%%Y-%%m-%%d"), interval 1 day)
        as max_day
    from {}
    """.format(table_name)
    return sql
