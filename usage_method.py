"""
Author: yifan@netopstec.com

Create date: 2018-02-06

Description: 封装常用的方法

Contain:
    1. 初始化定义表名称
    2. 定义df字段顺序

Update:

"""

import time
import pandas as pd
import usage_sql


# 1. 定义表名称
def init_table_name(name):

    # 获取当前日期字符串
    today = time.strftime('%Y%m%d', time.localtime())

    table = today + name
    return table


# 2. 定义df字段顺序
def init_columns():

    # 一共28个字段
    columns = ['tid', 'type', 'status', 'trade_from', 'buyer_nick',  'receiver_mobile',
               'receiver_state',   'created', 'pay_time',
               'outer_iid', 'num_iid', 'outer_sku_id', 'sku_properties_name', 'title', 'num',
               'sales', 'total_fee', 'price', 'divide_order_fee', 'payment',  'post_fee'
               ]

    return columns


# 3. 获取最大的日期
def order_last_day(con, table_name):

    df_day = pd.read_sql(usage_sql.get_last_day(table_name), con)
    max_day = df_day['max_day'].iloc[0]

    return max_day


# 4. 获取今日日期
def get_today():

    today = time.strftime('%Y-%m-%d', time.localtime())

    return today
