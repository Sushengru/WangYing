"""
    目的： 封装连接数据库的方法

"""

import pymysql
from sqlalchemy import create_engine

# yifan 数据库配置
config_local = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '060418ssr',
    'db': 'yifan',
    'charset': 'utf8',
    'cursorclass': pymysql.cursors.DictCursor
}

# user_tag 数据库配置
config_local_usertag = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '060418ssr',
    'db': 'user_tag',
    'charset': 'utf8',
    'cursorclass': pymysql.cursors.DictCursor
}

# BI brand 数据库配置
config_bi_brand = {
    'host': '192.168.12.182',
    'port': 3306,
    'user': 'yifan',
    'password': 'yifan',
    'db': 'brand',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}


config_brand_pymysql = {
    'host': '192.168.15.138',
    'port': 3306,
    'user': 'yifan',
    'password': 'yifan',
    'db': 'brand',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}



# brand数据库
# def db_brand():
#     conn_local = pymysql.connect(**config_bi_brand)
#     return conn_local
#
#
# def db_brand2():
#     connstr = 'mysql+pymysql://yifan:yifan@192.168.12.182/brand?charset=utf8mb4'
#     engine = create_engine(connstr, echo=True)
#     return engine


# yifan数据库
def db_local_yifan():
    conn_local = pymysql.connect(**config_local)
    return conn_local


def db_local_yifan2():
    connstr = 'mysql+pymysql://root:060418ssr@localhost/yifan?charset=utf8'
    engine = create_engine(connstr, echo=True)
    return engine


# user_tag数据库
def db_local_usertag():
    conn_local = pymysql.connect(**config_local_usertag)
    return conn_local


# 服务器数据库
def db_brand_pymysql():
    conn_local = pymysql.connect(**config_brand_pymysql)
    return conn_local


# 自定义数据库链接（导入用）
def db_conn(schema_name):
    connstr = 'mysql+pymysql://root:060418ssr@localhost/{}?charset=utf8'.format(schema_name)
    engine = create_engine(connstr, echo=True)
    return engine


# crm_db 数据库配置
crm_db = {
    'host': '192.168.1.236',
    'port': 3306,
    'user': 'tang',
    'password': 'wytec',
    'db': 'crm_db',
    'charset': 'utf8',
    'cursorclass': pymysql.cursors.DictCursor
}


# yifan数据库
def db_crm():
    conn_local = pymysql.connect(**crm_db)
    return conn_local


def db_crm2():
    connstr = 'mysql+pymysql://tang:wytec@192.168.1.236/crm_db?charset=utf8'
    engine = create_engine(connstr, echo=True)
    return engine


def db_longyi():
    connstr = 'mysql+pymysql://guest:05160824@192.168.13.95/ddmx?charset=utf8'
    engine = create_engine(connstr, echo=True)
    return engine


def db_brand():
    connstr = 'mysql+pymysql://yifan:yifan@192.168.15.138/brand?charset=utf8'
    engine = create_engine(connstr, echo=True)
    return engine


# 封装用pymysql进行数据查询
def query_sql(db_connect, sql):
    try:
        with db_connect.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
    finally:
        pass
    return result


# 封装查询操作，commit查询操作，不返回result
def query_operation(db_connect, sql):
    try:
        with db_connect.cursor() as cur:
            cur.execute(sql)
            print(sql)
            db_connect.commit()
            print('commit complete')
    finally:
        pass
