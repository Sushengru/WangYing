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
    path = "G:\python\数据库\product_table\美素_product.xlsx"

    # 订单表序号 和 店铺id
    table_num, visitor_id = '5', '1638195672'

    # 用于数据清洗筛除不要的数据
    key = ['正品', '新客专享', '买大送小', '新品']
    # # 用于商品表的字段排序
    # cloums = ['add_time', 'outer_iid', 'num_iid', 'type', 'rank', 'num', 'weight', 'short_name', 'combine_name']

    # 表的注释
    table_comment = ['美素产品表', '美素金装订单数据表', '美素金装会员统计表', '美素金装删除的订单', '美素金装会员信息表']
    # 数据表名
    table_name = ['friso_product', 'friso_gold', 'friso_gold_stat', 'friso_gold_throw', 'friso_gold_buyer_information']

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
    # today="2019-05-6"
    # # try:
    last_day = usage_method.order_last_day(create_con, table_name[1])
    # # except:
    # last_day="2010-05-06"
    # # last_day = '2013-01-01'
    # today = '2019-03-05'
    print(last_day, today)
    # print(db_crm,create_con,conn)

    # 1. 创建数据库表
    # option_function.create_order_table(create_con, table_name, table_comment)
    product_creat.create_friso_product(create_con, table_name)
    stat_creat.create_milk_stat(create_con, table_name, table_comment)

    # 2. 读取商品表
    df_product = option_function.read_product(path)
    print("商品部读取成功")
    # 3. 读取订单数据
    gold = option_function.get_brand(db_crm, table_num, visitor_id, last_day, today)
    print("订单数据读取成功")
    # #
    # 3. 读取会员数据
    df_gold_buyer = option_function.get_gold_buyer(gold, df_product)
    print("会员数据读取成功")

    # 4. 清洗订单数据
    df_gold, df_gold_throw = clean_method.clean_friso_gold(gold, df_product)
    print("订单清洗成功")

    # 5. 导入到数据库
    pd.io.sql.to_sql(df_product, table_name[0], conn, schema='brand', index=False, if_exists='append')
    print("产品表导入成功")
    pd.io.sql.to_sql(df_gold, table_name[1], conn, schema='brand', index=False, if_exists='append')
    print("订单表导入成功")
    pd.io.sql.to_sql(df_gold_throw, table_name[3], conn, schema='brand', index=False, if_exists='append')
    print("删除的订单导入成功")
    pd.io.sql.to_sql(df_gold_buyer, table_name[4], conn, schema='brand', index=False, if_exists='append')
    print("会员表导入成功")

    # 6. 生成统计数据表
    df_stat = brand_stat.milk_stat(create_con, table_name[1], table_name[0])
    pd.io.sql.to_sql(df_stat, table_name[2], conn, schema='brand', index=False, if_exists='append')
    print("统计表解析成功")
    print("统计表导入成功")
    # print(df_stat)

    # 7. buyer_id更新到订单表
    option_function.update_id(create_con, table_name[1], table_name[2])
    print("finished!")
