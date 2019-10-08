import time
from 数据库.current_method import usage_method
from 数据库.current_method import option_function
from 数据库.SQL_connect import db_connect
from 数据库.current_method import clean_method
from 数据库.product_table import product_creat
from 数据库.current_method import stat_creat
from 数据库.stat_table import brand_stat

# 执行
if __name__ == '__main__':
    # 商品表路径
    path = "G:\python\数据库\product_table\好侍_product.xlsx"

    # 订单表序号 和 店铺id
    table_num, visitor_id = '4', '2494156515'

    # 用于数据清洗筛除不要的数据
    key = ['优惠券', '1元福袋', '邮费补拍', '赠品', '积分兑换', '付邮试用', '测试', '邮费', '一元抢', '秒杀', '1元抢']

    # 表的注释
    table_comment = ['好侍产品表', '好侍订单数据表', '好侍会员统计表', '好侍删除的订单', '好侍会员信息表']
    # 数据表名
    table_name = ['house_product', 'house', 'house_stat', 'house_throw', 'house_buyer_information']

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
    last_day = usage_method.order_last_day(create_con, 'house')
    # except:
    # last_day="2015-1-1"
    # today="2018-11-12"
    print(last_day, today)
    # print(db_crm,create_con,conn)

    # 1. 创建数据库表
    # option_function.create_order_table(create_con, table_name, table_comment)
    product_creat.create_house_product(create_con, table_name)
    stat_creat.create_brand_stat(create_con, table_name, table_comment)

    # 2. 读取商品表
    df_product = option_function.read_product(path)
    print(df_product)

    # 3. 读取订单数据
    house = option_function.get_brand(db_crm, table_num, visitor_id, last_day, today)
    print(house)
    #
    # 3. 读取会员数据
    df_house_buyer = option_function.get_buyer(house)
    print(df_house_buyer)

    # 4. 清洗订单数据
    df_house, df_house_throw = clean_method.clean_house(house, df_product)
    print(df_house_throw)
    print(df_house)

    # 5. 导入到数据库
    df_product.to_sql(table_name[0], conn, schema='brand', index=False, if_exists='append')
    print("导入成功")
    df_house.to_sql(table_name[1], conn, schema='brand', index=False, if_exists='append')
    print("导入成功")
    df_house_throw.to_sql(table_name[3], conn, schema='brand', index=False, if_exists='append')
    print("导入成功")
    df_house_buyer.to_sql(table_name[4], conn, schema='brand', index=False, if_exists='append')
    print("导入成功")

    # 6. 生成统计数据表
    df_stat = brand_stat.house_stat(create_con, table_name[1], table_name[0])
    df_stat.to_sql(table_name[2], conn, schema='brand', index=False, if_exists='append')
    print("导入成功")
    # print(df_stat)

    # 7. buyer_id更新到订单表
    option_function.update_id(create_con, table_name[1], table_name[2])
    print("finished!")
