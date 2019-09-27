"""
Author: yifan@netopstec.com

Create_time: 2018-04-12

Description: triumph订单清洗、生成统计商品表

Process:
    1. 创建数据库表
    2. 读取商品表
    3. 读取订单数据
    4. 清洗订单数据
    5. 导入到数据库
    6. 生成统计数据表

Update:


"""

from DB_Connect import db_connect
# from triumph import triumph_sql
import pandas as pd
from pandas import DataFrame
import usage_method, usage_sql
import time



def read_product(file_path, product_name, conn, schema):
    """
    :desc
        读取商品表，将增量数据存到数据库中，triumph_product_add 是关键
        
    :param
        file_path: 商品excel文件路径
        product_name: 数据库商品表名称
        conn: 本地数据库连接
        schema: 数据库名
        
    :return
        商品信息数据(df)
    """

    triumph_product = pd.read_excel(file_path, dtype={'num_iid': str})
    print(triumph_product.head())
    print(triumph_product.dtypes)

    # 根据商品数据添加时间， 筛选增量导入到数据库
    execute_time = time.strftime('%Y%m%d', time.localtime())
    triumph_product_add = triumph_product[triumph_product['add_time'] >= execute_time]

    if len(triumph_product_add) != 0:
        triumph_product.to_sql(product_name,
                               con=conn,
                               schema=schema,
                               index=False,
                               if_exists='append')

    return triumph_product


def get_triumph(conn, oder_table_num, visitor, s_time, e_time):
    """
    :desc
        读取订单表数据

    :param
        conn: 数据库连接，网营数据库
        oder_table_num: 订单表的序号， 例如 0,对应 tb_order_0
        visitor: visitor_id

    :return:
        订单数据(df)

    """

    # DataFrame字段顺序
    col = usage_method.init_columns()

    # 获取查询订单sql，查询返回DF
    sql_order = usage_sql.get_order(oder_table_num, visitor, s_time, e_time)
    result = db_connect.query_sql(conn, sql_order)
    df_order = DataFrame(result, columns=col)

    print(df_order.head(20))

    return df_order


def clean_triumph(df_triumph, key_words):
    """
    :desc
        清洗订单数据

    :param
        df_triumph: triumph订单数据(df)
        df_product: 商品数据(df)
        key_words: 清洗关键词

    :return:
        df

    """
    # 1. 替换NAN为空
    df_triumph['outer_sku_id'].fillna("", inplace=True)

    # 2. 添加新的金额字段，修正金额问题
    df_triumph['sales'] = df_triumph['payment']
    df_triumph.loc[df_triumph['pay_time'] >= '2017-11-11', 'sales'] = df_triumph['divide_order_fee']

    # 3. 删除交易关闭订单
    # df_triumph = df_triumph[df_triumph['status'] != 'TRADE_CLOSED']
    print(df_triumph)
    # 4. 删除交易金额为0元的
    df_triumph = df_triumph[df_triumph['sales'] > 0]
    # 5. 删除2000年的订单
    df_triumph = df_triumph[df_triumph['pay_time'] > '2012-01-01']

    # 6. 根据关键词，筛选掉不需要的订单数据: '优惠券', '邮费', '测试',
    for i in range(len(key_words)):
        df_triumph = df_triumph[~ df_triumph['title'].str.contains(key_words[i])]

    # 7. 按订单支付时间排序
    df_triumph.sort_values(axis=0, by='pay_time', inplace=True, ascending=True)

    return df_triumph


def create_triumph(conn, triumph_name, product_name, stat_name, comment_list):
    """
    :desc
        创建数据库表,用于后续存储数据

    :param
        conn: 本地数据库连接
        triumph_name: 数据库订单表名称
        product_name: 数据库商品表名称
        stat_name: 数据库会员统计表名称
        comment_list: list[0]是订单表注释，list[1]是会员统计表注释

    """

    # 创建产品表（只需使用一次）
    # sql_create_product = triumph_sql.create_triumph_product(product_name)
    # db_connect.query_operation(conn, sql_create_product)

    # 获取创建订单表和统计表语句
    sql_create_triumph = usage_sql.create_order_table(triumph_name, comment_list[0])
    sql_create_stat = usage_sql.create_stat(stat_name, comment_list[1])
    # print(sql_create_stat)
    # 执行sql
    # db_connect.query_operation(conn, sql_create_stat)
    db_connect.query_operation(conn, sql_create_triumph)


def stat_option(conn, triumph_name):
    """
    :desc
        查询统计数据

    :param
        conn: 本地数据库连接
        triumph_name: 数据库订单表名称

    :return:
        会员统计数据(df)

    """

    # 获取统计查询语句
    sql_stat1 = usage_sql.order_stat1(triumph_name)
    sql_stat2 = usage_sql.order_stat2(triumph_name)

    # 执行查询
    # result1 = db_connect.query_sql(conn, sql_stat1)
    # result2 = db_connect.query_sql(conn, sql_stat2)

    # 转存到DataFrame里
    df_stat1 = pd.read_sql(sql_stat1, conn)
    df_stat2 = pd.read_sql(sql_stat2, conn)

    # 连接DataFrame
    df_stat1.merge(df_stat2, on=['buyer_nick'], how='left')

    print(df_stat1.dtypes)
    print(df_stat1.head())
    return df_stat1


def update_id(conn, table_target, table_id):
    """
    :desc
        在订单表中更新buyer_id

    :param
        conn: 本地数据库连接
        table_target: 目标表，即订单表
        table_id: buyer_id表，即会员统计表

    """

    # 获取更新语句
    sql_update = usage_sql.update_buyer_id(table_target, table_id)
    print(sql_update)
    # 执行语句
    db_connect.query_operation(conn, sql_update)


# 执行
if __name__ == '__main__':

    """
    输入相关参数
    """
    # a. 商品表路径（目前已不用）
    # path = 'G:\一帆文件\数据清洗\\triumph\\20180322_triumph商品表.xlsx'

    # b. 服务器订单表id 和 店铺id
    table_num = '3'
    visitor_id = '929347050'

    # c. 关键词，用于筛选订单
    key_word = ['优惠券', '邮费', '测试', '拍下无效', '赠品', '无门槛', '积分', '抽奖', '定制礼盒', '包邮']

    # d. 获取数据库表名
    triumph_table = 'triumph'       # 清洗后的订单表名
    triumph_stat = 'triumph_stat'   # 计算生成的统计表名

    # e. 表注释
    comment_list = ['triumph订单数据表', 'triumph会员统计表']

    # f. 连接数据库
    db_crm = db_connect.db_crm()                 # 订单服务器库, py_mysql
    create_con = db_connect.db_brand_pymysql()   # 本地数据库, py_mysql
    tosql_con = db_connect.db_brand()            # 本地数据库, sql_alchemy

    # g. 时间范围
    start_day = usage_method.order_last_day(tosql_con, triumph_table)
    end_day = time.strftime('%Y-%m-%d', time.localtime())

    """
    下面开始运行
    ----------------------------------------------------------------------------------------------------------  
    """

    # 1. 创建数据库表（目前已不用）
    # create_triumph(create_con, triumph_table, product_table, triumph_stat, comment_list)

    # 2. 读取商品表（目前已不用）
    # product = read_product(path, product_table, tosql_con, 'user_tag')

    # 3. 读取订单数据
    triumph = get_triumph(db_crm, table_num, visitor_id, start_day, end_day)

    # 4. 清洗订单数据
    triumph_clean = clean_triumph(triumph, key_word)

    # 5. 导入到数据库
    triumph_clean = triumph_clean[usage_method.init_columns()]
    triumph_clean.to_sql(triumph_table, tosql_con, schema='brand', index=False, if_exists='append')

    # 6. 生成统计数据表
    df_stat = stat_option(create_con, triumph_table)
    df_stat.to_sql(triumph_stat, tosql_con, schema='brand', index=False, if_exists='append')

    # 7. buyer_id更新到订单表
    update_id(create_con, triumph_table, triumph_stat)


