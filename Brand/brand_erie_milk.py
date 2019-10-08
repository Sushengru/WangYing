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
    path = "G:\python\数据库\product_table\伊利旗舰店_product.xlsx"

    # 订单表序号 和 店铺id
    table_num, visitor_id = '4', '299244686'

    # 用于数据清洗筛除不要的数据
    key = ['金典', '畅意', 'QQ星', '成人奶粉', '安慕希', '谷粒多', '舒化', '味可滋', '拜拜君', '奶片', '核桃乳',
           '培兰', '邮费', '优酸乳', '米粉', '植选', '麦片', '无菌砖', '婴幼儿奶粉', '奶茶粉', '奶酪', '焕醒源', '柏菲兰', '金典娟姗']
    # 用于商品表的字段排序
    # cloums = ['add_time', 'outer_iid', 'num_iid', 'type', 'rank', 'num', 'weight', 'short_name', 'combine_name']

    # 表的注释
    table_comment = ['伊利旗舰店产品表', '伊利旗舰店订单数据表', '伊利旗舰店会员统计表',
                     '伊利旗舰店删除的订单', '伊利旗舰店会员信息表']
    # 数据表名
    table_name = ['erie_milk_product', 'erie_milk', 'erie_milk_stat',
                  'erie_milk_throw', 'erie_milk_buyer_information']

    # 连接数据库
    db_crm = db_connect.db_crm()  # CRM数据库, pymysql
    create_con = db_connect.db_local_usertag()  # 本地数据库, pymysql
    conn = db_connect.db_yf_connect()  # 本地数据库, sqlalchemy
    create_con2 = db_connect.db_local_usertag()

    # db_crm = db_connect.db_crm()  # CRM数据库, pymysql
    # create_con = db_connect.db_local_ly()  # 本地数据库, pymysql
    # conn = db_connect.db_ly_connect() # 本地数据库, sqlalchemy

    # 查询的时间范围
    # today = time.strftime('%Y-%m-%d', time.localtime())
    today = "2019-1-20"
    # try:
    # last_day = usage_method.order_last_day(create_con,table_name[1])
    # except:
    # last_day="2018-10-18 10:19:56"
    last_day = '2008-01-01'
    # today = '2018-07-01'
    print(last_day, today)
    # print(db_crm,create_con,conn)

    # 1. 创建数据库表
    # option_function.create_order_table(create_con, table_name, table_comment)
    # product_creat.create_erie_milk_product(create_con, table_name)
    # stat_creat.create_brand_stat(create_con, table_name,table_comment)

    # 2. 读取商品表
    # df_product = option_function.read_erie_milk_product(path)
    # print(df_product)

    # 3. 读取订单数据
    # erie_milk_now = option_function.get_brand(db_crm, table_num, visitor_id, last_day, today)
    # erie_milk_before=option_function.get_erie_milk_before(create_con2)
    # erie_milk=pd.concat([erie_milk_before,erie_milk_now])
    # # print(erie_milk)

    # 3. 读取会员数据
    # df_erie_milk_buyer = option_function.get_buyer(erie_milk)
    # print(df_erie_milk_buyer)

    # 4. 清洗订单数据
    # df_erie_milk,df_erie_milk_throw = clean_method.clean_erie_milk(erie_milk,df_product,key)
    # print(df_erie_milk)
    # print(df_erie_milk_throw)

    # 5. 导入到数据库
    # df_product.to_sql(table_name[0], conn, schema='user_tag', index=False, if_exists='append')
    # print("导入成功")
    # df_erie_milk.to_sql(table_name[1], conn, schema='user_tag', index=False, if_exists='append')
    # print("导入成功")
    # df_erie_milk_throw.to_sql(table_name[3], conn, schema='user_tag', index=False, if_exists='append')
    # print("导入成功")
    # df_erie_milk_buyer.to_sql(table_name[4], conn, schema='user_tag', index=False, if_exists='append')
    # print("导入成功")

    # 6. 生成统计数据表
    df_stat = brand_stat.brand_stat(create_con, table_name[1], table_name[0])
    df_stat.to_sql(table_name[2], conn, schema='user_tag', index=False, if_exists='append')
    print("导入成功")
    # print(df_stat)

    # 7. buyer_id更新到订单表
    option_function.update_id(create_con, table_name[1], table_name[2])
    print("finished!")
