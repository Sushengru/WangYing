import datetime
import pandas as pd
from 数据库.SQL import stat_sql
import time


def brand_stat(conn, table_list1, table_list2):
    sql_brand = stat_sql.get_brand(table_list1)
    sql_brand_product = stat_sql.get_brand_product(table_list1, table_list2)
    df_brand = pd.read_sql(sql_brand, conn)
    df_brand_product = pd.read_sql(sql_brand_product, conn)
    df_brand['date_month'] = df_brand['pay_time'].map(lambda x: str(x)[0:7])
    df_brand['date_day'] = df_brand['pay_time'].map(lambda x: str(x)[0:10])

    # 去除同一天购买相同类目
    df_brand_product['date_day'] = df_brand_product['pay_time'].map(lambda x: str(x)[0:10])
    df_brand_product = df_brand_product.drop_duplicates(subset=['buyer_nick', 'date_day', 'category'], keep='first')

    # 计算首次购买的类目
    df_brand_product['rank_num'] = df_brand_product['date_day'].groupby(df_brand_product['buyer_nick']).rank(
        ascending=1, method='dense')
    df_first_date = df_brand_product[df_brand_product['rank_num'] == 1.0]
    # df_state_product = df_first_date['category'].groupby(df_first_date['buyer_nick']).unique().reset_index()
    df_stat_product = df_first_date['category'].groupby(df_first_date['buyer_nick']).aggregate(
        lambda x: ','.join(x)).reset_index()

    # #计算最早、最晚、总额
    df_stat = df_brand.groupby(['buyer_nick'], as_index=False).agg(
        {'pay_time': ['min', 'max'], 'date_month': ['min'], 'sales': ['sum']})
    df_stat.columns = df_stat.columns.droplevel()
    df_stat.columns = ['buyer_nick', 'first_purchase_time', 'last_purchase_time', 'first_purchase_month',
                       'total_payment']
    # print(df_state)

    # 计算距今购买时间
    today = datetime.date.today()
    df_stat['first_away'] = today - pd.to_datetime(df_stat['first_purchase_time'])
    df_stat['first_away'] = df_stat['first_away'].astype(str).str.extract(pat='^(\d+).*?', expand=False)

    df_stat['last_away'] = today - pd.to_datetime(df_stat['last_purchase_time'])
    df_stat['last_away'] = df_stat['last_away'].astype(str).str.extract(pat='^(\d+).*?', expand=False)
    # print(df_state)

    # 计算客单价
    df_stat['times'] = df_brand.drop_duplicates(subset=['buyer_nick', 'date_day'], keep='first').groupby('buyer_nick',
                                                                                                         as_index=False).count()[
        'date_day']
    df_stat['avg_paypct'] = (df_stat['total_payment'] / df_stat['times']).round(2)
    # 筛选首月数据
    df_brand['rank_num'] = df_brand['date_month'].groupby(df_brand['buyer_nick']).rank(ascending=1, method='dense')
    df_first_month = df_brand[df_brand['rank_num'] == 1.0]

    # #首月购买次数
    df_stat['first_count'] = \
    df_first_month.drop_duplicates(subset=['buyer_nick', 'date_day'], keep='first').groupby('buyer_nick',
                                                                                            as_index=False).count()[
        'date_day']
    # 首月消费额
    # print(df_state)
    df_stat['first_sum'] = df_first_month.groupby('buyer_nick', as_index=False).sum()['sales']
    # 首月客单价
    df_stat['first_month_paypct'] = (df_stat['first_sum'] / df_stat['first_count']).round(2)
    # print(df_state)

    # 余下月份客单价
    df_stat['surplus_month_paypct'] = (
                (df_stat['total_payment'] - df_stat['first_sum']) / (df_stat['times'] - df_stat['first_count'])).round(
        2)
    df_stat['surplus_month_paypct'].fillna(value=0, inplace=True)
    df_stat.drop(['first_count', 'first_sum'], axis=1, inplace=True)

    # 首次购买的类目
    df_stat['first_cate'] = df_stat_product['category']
    return df_stat


def milk_stat(conn, table_list1, table_list2):
    sql_brand = stat_sql.get_brand(table_list1)
    sql_brand_product = stat_sql.get_milk_product(table_list1, table_list2)
    df_brand = pd.read_sql(sql_brand, conn)
    df_brand_product = pd.read_sql(sql_brand_product, conn)
    # df_devon=pd.read_excel("C:\\Users\longyi\PycharmProjects\数据库\\brand_update\df_devon_throw.xlsx")
    df_brand['date_month'] = df_brand['pay_time'].map(lambda x: str(x)[0:7])
    df_brand['date_day'] = df_brand['pay_time'].map(lambda x: str(x)[0:10])

    # 去除同一天购买相同类目
    df_brand_product = df_brand_product.drop_duplicates(subset=['buyer_nick'], keep='first')
    df_brand_product = pd.DataFrame(df_brand_product, columns=['buyer_nick', 'rank'])
    df_brand_product.columns = ['buyer_nick', 'first_rank']
    # #计算首次购买的类目
    # df_brand_product['rank_num']=df_brand_product['date_day'].groupby(df_brand_product['buyer_nick']).rank(ascending=1,method='dense')
    # df_first_date=df_brand_product[df_brand_product['rank_num']==1.0]
    # # df_state_product = df_first_date['rank'].groupby(df_first_date['buyer_nick']).unique().reset_index()
    # df_stat_product = df_first_date['rank'].groupby(df_first_date['buyer_nick']).aggregate(lambda x:','.join(x)).reset_index()

    # #计算最早、最晚、总额
    df_stat = df_brand.groupby(['buyer_nick'], as_index=False).agg(
        {'pay_time': ['min', 'max'], 'date_month': ['min'], 'sales': ['sum']})
    df_stat.columns = df_stat.columns.droplevel()
    df_stat.columns = ['buyer_nick', 'first_purchase_time', 'last_purchase_time', 'first_purchase_month',
                       'total_payment']
    # print(df_state)

    # 计算距今购买时间
    # today=datetime.date.today()
    df_stat['today'] = time.strftime('%Y-%m-%d', time.localtime())
    df_stat['first_away'] = pd.to_datetime(df_stat['today']) - pd.to_datetime(df_stat['first_purchase_time'])
    df_stat['first_away'] = df_stat['first_away'].astype(str).str.extract(pat='^(\d+).*?', expand=False)
    # print(df_state)
    #
    df_stat['last_away'] = pd.to_datetime(df_stat['today']) - pd.to_datetime(df_stat['last_purchase_time'])
    df_stat['last_away'] = df_stat['last_away'].astype(str).str.extract(pat='^(\d+).*?', expand=False)
    df_stat.drop(['today'], axis=1, inplace=True)
    # print(df_state)

    # 计算客单价
    df_stat['times'] = df_brand.drop_duplicates(subset=['buyer_nick', 'date_day'], keep='first').groupby('buyer_nick',
                                                                                                         as_index=False).count()[
        'date_day']
    df_stat['avg_paypct'] = (df_stat['total_payment'] / df_stat['times']).round(2)
    # 筛选首月数据
    df_brand['rank_num'] = df_brand['date_month'].groupby(df_brand['buyer_nick']).rank(ascending=1, method='dense')
    df_first_month = df_brand[df_brand['rank_num'] == 1.0]

    # #首月购买次数
    df_stat['first_count'] = \
    df_first_month.drop_duplicates(subset=['buyer_nick', 'date_day'], keep='first').groupby('buyer_nick',
                                                                                            as_index=False).count()[
        'date_day']
    # 首月消费额
    # print(df_state)
    df_stat['first_sum'] = df_first_month.groupby('buyer_nick', as_index=False).sum()['sales']
    # 首月客单价
    df_stat['first_month_paypct'] = (df_stat['first_sum'] / df_stat['first_count']).round(2)
    # print(df_state)

    # 余下月份客单价
    df_stat['surplus_month_paypct'] = (
                (df_stat['total_payment'] - df_stat['first_sum']) / (df_stat['times'] - df_stat['first_count'])).round(
        2)
    df_stat['surplus_month_paypct'].fillna(value=0, inplace=True)
    df_stat.drop(['first_count', 'first_sum'], axis=1, inplace=True)

    # 首次购买的类目
    df_stat = pd.merge(df_stat, df_brand_product, how="left", on="buyer_nick")
    return df_stat


def erie_stat(conn, table_list1, table_list2):
    sql_brand = stat_sql.get_brand(table_list1)
    sql_brand_product = stat_sql.get_erie_product(table_list1, table_list2)
    df_brand = pd.read_sql(sql_brand, conn)
    df_brand_product = pd.read_sql(sql_brand_product, conn)
    # df_devon=pd.read_excel("C:\\Users\longyi\PycharmProjects\数据库\\brand_update\df_devon_throw.xlsx")
    df_brand['date_month'] = df_brand['pay_time'].map(lambda x: str(x)[0:7])
    df_brand['date_day'] = df_brand['pay_time'].map(lambda x: str(x)[0:10])

    # 去除同一天购买相同类目
    df_brand_product = df_brand_product.drop_duplicates(subset=['buyer_nick'], keep='first')
    df_brand_product = pd.DataFrame(df_brand_product, columns=['buyer_nick', 'rank'])
    df_brand_product.columns = ['buyer_nick', 'first_rank']
    # #计算首次购买的类目
    # df_brand_product['rank_num']=df_brand_product['date_day'].groupby(df_brand_product['buyer_nick']).rank(ascending=1,method='dense')
    # df_first_date=df_brand_product[df_brand_product['rank_num']==1.0]
    # # df_state_product = df_first_date['rank'].groupby(df_first_date['buyer_nick']).unique().reset_index()
    # df_stat_product = df_first_date['rank'].groupby(df_first_date['buyer_nick']).aggregate(lambda x:','.join(x)).reset_index()

    # #计算最早、最晚、总额
    df_stat = df_brand.groupby(['buyer_nick'], as_index=False).agg(
        {'pay_time': ['min', 'max'], 'date_month': ['min'], 'sales': ['sum']})
    df_stat.columns = df_stat.columns.droplevel()
    df_stat.columns = ['buyer_nick', 'first_purchase_time', 'last_purchase_time', 'first_purchase_month',
                       'total_payment']
    # print(df_state)

    # 计算距今购买时间
    today = datetime.date.today()
    df_stat['first_away'] = today - pd.to_datetime(df_stat['first_purchase_time'])
    df_stat['first_away'] = df_stat['first_away'].astype(str).str.extract(pat='^(\d+).*?', expand=False)
    # print(df_state)
    #
    df_stat['last_away'] = today - pd.to_datetime(df_stat['last_purchase_time'])
    df_stat['last_away'] = df_stat['last_away'].astype(str).str.extract(pat='^(\d+).*?', expand=False)
    # print(df_state)

    # 计算客单价
    df_stat['times'] = df_brand.drop_duplicates(subset=['buyer_nick', 'date_day'], keep='first').groupby('buyer_nick',
                                                                                                         as_index=False).count()[
        'date_day']
    df_stat['avg_paypct'] = (df_stat['total_payment'] / df_stat['times']).round(2)
    # 筛选首月数据
    df_brand['rank_num'] = df_brand['date_month'].groupby(df_brand['buyer_nick']).rank(ascending=1, method='dense')
    df_first_month = df_brand[df_brand['rank_num'] == 1.0]

    # #首月购买次数
    df_stat['first_count'] = \
    df_first_month.drop_duplicates(subset=['buyer_nick', 'date_day'], keep='first').groupby('buyer_nick',
                                                                                            as_index=False).count()[
        'date_day']
    # 首月消费额
    # print(df_state)
    df_stat['first_sum'] = df_first_month.groupby('buyer_nick', as_index=False).sum()['sales']
    # 首月客单价
    df_stat['first_month_paypct'] = (df_stat['first_sum'] / df_stat['first_count']).round(2)
    # print(df_state)

    # 余下月份客单价
    df_stat['surplus_month_paypct'] = (
                (df_stat['total_payment'] - df_stat['first_sum']) / (df_stat['times'] - df_stat['first_count'])).round(
        2)
    df_stat['surplus_month_paypct'].fillna(value=0, inplace=True)
    df_stat.drop(['first_count', 'first_sum'], axis=1, inplace=True)

    # 首次购买的类目
    df_stat = pd.merge(df_stat, df_brand_product, how="left", on="buyer_nick")
    return df_stat


def dr_browns_stat(conn, table_list1, table_list2):
    sql_brand = stat_sql.get_brand(table_list1)
    sql_brand_product = stat_sql.get_dr_browns_product(table_list1, table_list2)
    df_brand = pd.read_sql(sql_brand, conn)
    df_brand_product = pd.read_sql(sql_brand_product, conn)
    # df_devon=pd.read_excel("C:\\Users\longyi\PycharmProjects\数据库\\brand_update\df_devon_throw.xlsx")
    df_brand['date_month'] = df_brand['pay_time'].map(lambda x: str(x)[0:7])
    df_brand['date_day'] = df_brand['pay_time'].map(lambda x: str(x)[0:10])
    # print(df_brand)
    # print(df_brand_product)
    # 去除同一天购买相同类目
    # df_brand_product['date_day']=df_brand_product['pay_time'].map(lambda x: str(x)[0:10])
    # df_brand_product=df_brand_product.drop_duplicates(subset=['buyer_nick','date_day','category'],keep='first')
    # print(df_brand_product)
    # # 计算首次购买的类目
    # df_brand_product['rank_num']=df_brand_product['date_day'].groupby(df_brand_product['buyer_nick']).rank(ascending=1,method='dense')
    # df_first_date=df_brand_product[df_brand_product['rank_num']==1.0]
    # print(df_first_date)
    # # df_state_product = df_first_date['category'].groupby(df_first_date['buyer_nick']).unique().reset_index()
    # df_stat_product = df_first_date['category'].groupby(df_first_date['buyer_nick']).aggregate(lambda x:','.join(x)).reset_index()
    # print(df_stat_product)
    # 计算最早、最晚、总额
    df_stat = df_brand.groupby(['buyer_nick'], as_index=False).agg(
        {'pay_time': ['min', 'max'], 'date_month': ['min'], 'sales': ['sum']})
    df_stat.columns = df_stat.columns.droplevel()
    df_stat.columns = ['buyer_nick', 'first_purchase_time', 'last_purchase_time', 'first_purchase_month',
                       'total_payment']
    # print(df_state)

    # 计算距今购买时间
    today = datetime.date.today()
    df_stat['first_away'] = today - pd.to_datetime(df_stat['first_purchase_time'])
    df_stat['first_away'] = df_stat['first_away'].astype(str).str.extract(pat='^(\d+).*?', expand=False)
    # print(df_state)
    #
    df_stat['last_away'] = today - pd.to_datetime(df_stat['last_purchase_time'])
    df_stat['last_away'] = df_stat['last_away'].astype(str).str.extract(pat='^(\d+).*?', expand=False)
    # print(df_state)

    # 计算客单价
    df_stat['times'] = df_brand.drop_duplicates(subset=['buyer_nick', 'date_day'], keep='first').groupby('buyer_nick',
                                                                                                         as_index=False).count()[
        'date_day']
    df_stat['avg_paypct'] = (df_stat['total_payment'] / df_stat['times']).round(2)
    # 筛选首月数据
    df_brand['rank_num'] = df_brand['date_month'].groupby(df_brand['buyer_nick']).rank(ascending=1, method='dense')
    df_first_month = df_brand[df_brand['rank_num'] == 1.0]

    # #首月购买次数
    df_stat['first_count'] = \
    df_first_month.drop_duplicates(subset=['buyer_nick', 'date_day'], keep='first').groupby('buyer_nick',
                                                                                            as_index=False).count()[
        'date_day']
    # 首月消费额
    # print(df_state)
    df_stat['first_sum'] = df_first_month.groupby('buyer_nick', as_index=False).sum()['sales']
    # 首月客单价
    df_stat['first_month_paypct'] = (df_stat['first_sum'] / df_stat['first_count']).round(2)
    # print(df_state)

    # 余下月份客单价
    df_stat['surplus_month_paypct'] = (
                (df_stat['total_payment'] - df_stat['first_sum']) / (df_stat['times'] - df_stat['first_count'])).round(
        2)
    df_stat['surplus_month_paypct'].fillna(value=0, inplace=True)
    df_stat.drop(['first_count', 'first_sum'], axis=1, inplace=True)
    # print(df_stat)
    # #首次购买的类目
    # df_stat['first_cate']=df_stat_product['category']
    return df_stat


def house_stat(conn, table_list1, table_list2):
    sql_brand = stat_sql.get_brand(table_list1)
    sql_brand_product = stat_sql.get_house_product(table_list1, table_list2)
    df_brand = pd.read_sql(sql_brand, conn)
    df_brand_product = pd.read_sql(sql_brand_product, conn)
    df_brand['date_month'] = df_brand['pay_time'].map(lambda x: str(x)[0:7])
    df_brand['date_day'] = df_brand['pay_time'].map(lambda x: str(x)[0:10])

    # 去除同一天购买相同类目
    df_brand_product['date_day'] = df_brand_product['pay_time'].map(lambda x: str(x)[0:10])
    df_brand_product = df_brand_product.drop_duplicates(subset=['buyer_nick', 'date_day', 'category'], keep='first')

    # #计算首次购买的类目
    # df_brand_product['rank_num']=df_brand_product['date_day'].groupby(df_brand_product['buyer_nick']).rank(ascending=1,method='dense')
    # df_first_date=df_brand_product[df_brand_product['rank_num']==1.0]
    # # df_state_product = df_first_date['category'].groupby(df_first_date['buyer_nick']).unique().reset_index()
    # df_stat_product = df_first_date['category'].groupby(df_first_date['buyer_nick']).aggregate(lambda x:','.join(x)).reset_index()

    # #计算最早、最晚、总额
    df_stat = df_brand.groupby(['buyer_nick'], as_index=False).agg(
        {'pay_time': ['min', 'max'], 'date_month': ['min'], 'sales': ['sum']})
    df_stat.columns = df_stat.columns.droplevel()
    df_stat.columns = ['buyer_nick', 'first_purchase_time', 'last_purchase_time', 'first_purchase_month',
                       'total_payment']
    # print(df_state)

    # 计算距今购买时间
    today = datetime.date.today()
    df_stat['first_away'] = today - pd.to_datetime(df_stat['first_purchase_time'])
    df_stat['first_away'] = df_stat['first_away'].astype(str).str.extract(pat='^(\d+).*?', expand=False)

    df_stat['last_away'] = today - pd.to_datetime(df_stat['last_purchase_time'])
    df_stat['last_away'] = df_stat['last_away'].astype(str).str.extract(pat='^(\d+).*?', expand=False)
    # print(df_state)

    # 计算客单价
    df_stat['times'] = df_brand.drop_duplicates(subset=['buyer_nick', 'date_day'], keep='first').groupby('buyer_nick',
                                                                                                         as_index=False).count()[
        'date_day']
    df_stat['avg_paypct'] = (df_stat['total_payment'] / df_stat['times']).round(2)
    # 筛选首月数据
    df_brand['rank_num'] = df_brand['date_month'].groupby(df_brand['buyer_nick']).rank(ascending=1, method='dense')
    df_first_month = df_brand[df_brand['rank_num'] == 1.0]

    # #首月购买次数
    df_stat['first_count'] = \
    df_first_month.drop_duplicates(subset=['buyer_nick', 'date_day'], keep='first').groupby('buyer_nick',
                                                                                            as_index=False).count()[
        'date_day']
    # 首月消费额
    # print(df_state)
    df_stat['first_sum'] = df_first_month.groupby('buyer_nick', as_index=False).sum()['sales']
    # 首月客单价
    df_stat['first_month_paypct'] = (df_stat['first_sum'] / df_stat['first_count']).round(2)
    # print(df_state)

    # 余下月份客单价
    df_stat['surplus_month_paypct'] = (
                (df_stat['total_payment'] - df_stat['first_sum']) / (df_stat['times'] - df_stat['first_count'])).round(
        2)
    df_stat['surplus_month_paypct'].fillna(value=0, inplace=True)
    df_stat.drop(['first_count', 'first_sum'], axis=1, inplace=True)

    # #首次购买的类目
    # df_stat['first_cate']=df_stat_product['category']
    return df_stat
