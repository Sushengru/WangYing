import pandas as pd
from pandas import DataFrame
from 数据库.current_method import usage_method
from 数据库.SQL import usage_sql
from 数据库.current_method import option_function
import time
from 数据库.SQL_connect import db_connect
from 数据库.current_method import clean_method
from 数据库.product_table import product_creat
from 数据库.current_method import stat_creat
from 数据库.stat_table import brand_stat
import xlrd

# 执行
if __name__ == '__main__':
    # 商品表路径
    path = "G:\python\数据库\product_table\德运_product.xlsx"

    # 订单表序号 和 店铺id
    table_num, visitor_id = '4', '2894571536'

    # 用于数据清洗筛除不要的数据
    key = ['优惠券', '1元福袋', '邮费补拍', '赠品', '积分兑换', '付邮试用', '测试', '邮费', '一元抢', '秒杀', '1元抢']
    # # 用于商品表的字段排序
    # cloums = ['add_time', 'outer_iid', 'num_iid', 'type', 'rank', 'num', 'weight', 'short_name', 'combine_name']

    # 表的注释
    table_comment = ['德运产品表', '德运订单数据表', '德运会员统计表', '德运删除的订单', '德运会员信息表']
    # 数据表名
    table_name = ['devon_product', 'devon', 'devon_stat', 'devon_throw', 'devon_buyer_information']

    # 连接数据库
    db_crm = db_connect.db_crm()  # CRM数据库, pymysql
    # create_con = db_connect.db_local_usertag()  # 本地数据库, pymysql
    create_con = db_connect.db_server_usertag()  # 本地数据库, pymysql
    # conn = db_connect.db_yf_connect() # 本地数据库, sqlalchemy
    conn = db_connect.db_server_connect()  # 本地数据库, sqlalchemy
    # db_crm = db_connect.db_crm()  # CRM数据库, pymysql
    # create_con = db_connect.db_local_ly()  # 本地数据库, pymysql
    # conn = db_connect.db_ly_connect() # 本地数据库, sqlalchemy

    # 查询的时间范围

    today = time.strftime('%Y-%m-%d', time.localtime())
    # try:
    last_day = usage_method.order_last_day(create_con, table_name[1])
    # except:
    # last_day="2018-01-01"
    # last_day = '2010-01-01'
    # today = '2019-1-20'
    print(last_day, today)
    # print(db_crm,create_con,conn)

    # 1. 创建数据库表
    # option_function.create_order_table(create_con, table_name, table_comment)
    product_creat.create_devon_product(create_con, table_name)
    stat_creat.create_brand_stat(create_con, table_name, table_comment)

    # # 2. 读取商品表
    df_product = option_function.read_product(path)

    # 3. 读取订单数据
    devon = option_function.get_brand(db_crm, table_num, visitor_id, last_day, today)
    print(devon)

    # 3. 读取会员数据
    df_devon_buyer = option_function.get_buyer(devon)
    # print(df_devon_buyer)

    # # 4. 清洗订单数据
    df_devon, df_devon_throw = clean_method.clean_devon(devon, key)
    print(df_devon)
    print(df_devon_throw)

    # # 5. 导入到数据库
    df_product.to_sql(table_name[0], conn, schema='brand', index=False, if_exists='append')
    print("导入成功")
    df_devon.to_sql(table_name[1], conn, schema='brand', index=False, if_exists='append')
    print("导入成功")
    df_devon_throw.to_sql(table_name[3], conn, schema='brand', index=False, if_exists='append')
    print("导入成功")
    df_devon_buyer.to_sql(table_name[4], conn, schema='brand', index=False, if_exists='append')
    print("导入成功")

    # 6. 生成统计数据表
    df_stat = brand_stat.brand_stat(create_con, table_name[1], table_name[0])
    df_stat.to_sql(table_name[2], conn, schema='brand', index=False, if_exists='append')

    # # 7. buyer_id更新到订单表
    option_function.update_id(create_con, table_name[1], table_name[2])
    print("finished!")
