# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 11:07:12 2019

@author: EX-DUANLIAN002
"""

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import time
import logging
from datetime import timedelta,datetime
import calendar

_database = {'d0qer':{'password':'Paic2018','port':7616,'ip':'10.20.131.60','name':'d0qer','user':'dlmopr'},
             "kyfm":{"password":"Paic2019","port":7658,"ip":"30.31.0.182","name":"kyfm","user":"kyfmdata"}}

#global environ_type
#environ_type = 'pyxis'  
environ_type = 'local'


def func_times(func):
    """此函数用作统计运行时间"""
    def wapper(*args,**kargs):
        start = time.time()
        result = func(*args,**kargs)
        end = time.time()
        times = end-start
        print(f'{func.__name__} run times {times} s')
        return result
    return wapper

#连接数据库

def initial_engine(db,dbname):
    env = db[dbname]
    engine = create_engine('postgresql+psycopg2://{user}:{password}@{ip}:{port}/{name}'.format(**env))
    return engine

#得到DataFrame格式的数据库数据

def get_data(engine,sql=None):
    if environ_type == 'local':
        data_result = pd.read_sql_query(sql,engine)
    elif environ_type == 'pyxis':
        data_result = query_db(sql,engine)
    return data_result

def sql_execute(sql, engine):
    if environ_type == 'pyxis':
        return db_execute(sql, data_source_name = engine)
    elif environ_type == 'local':
        return engine.execute(sql)

#主键、唯一键字段转换，防止新增数据主键冲突    
def pu_convert(df):
    if df.字段类型.startswith('VARCHAR'):
        return df.字段英文名+"||'test'"
    elif df.字段类型.startswith('SERIAL'):
        return "nextval('"+table_name+"_pid_seq'::regclass)"
    else:
        return df.字段英文名
    
def get_counts(table_name):
    sql_counts = f"""select count(*) from {table_name}"""
    return list(sql_execute(sql_counts,engine))[0][0]
    
#利用表结构信息向目标表插入一条数据
def insert_data(table_name,table_structure):
    #原始字段
    insert_to = ','.join(table_structure.字段英文名)
    #获取主键和唯一键
    table_uk = table_structure[table_structure['是否主键'].isin(['PK','UK'])]
    #主键、唯一键字段转换，防止新增数据主键冲突
    uk_new = table_uk.apply(pu_convert,axis=1)
    table_uk['字段英文名'] = uk_new
    table_structure[table_structure['是否主键'].isin(['PK','UK'])] = table_uk
    to_insert = ','.join(table_structure.字段英文名)
    #生成造数语句
    insert_sql = f"""insert into {table_name} ({insert_to}) 
                     select {to_insert} from {table_name} where 
                     pid = (select max(pid) from {table_name})"""
    
    before_insert = get_counts(table_name)
    if before_insert == 0:
        print('目标表没有数')
        return Flase
    sql_execute(insert_sql,engine)
    after_insert = get_counts(table_name)
    if after_insert - before_insert == 1:
        print('成功插入一条数')
        return True
    else:
        print('造数失败')
        return False
    
    
def backflush(table_name,sql_func):
    sql = f"""select count(*) from pyxis_delete_log  where TABLENAME = '{table_name}'
"""
    _before = list(sql_execute(sql,engine))[0][0]
    _excute = f"""select {sql_func}()"""
    
    _backflush = sql_execute(_excute,engine)
    print(list(_backflush))
    _after = list(sql_execute(sql,engine))[0][0]
    
    if _after - _before == 1:
        print('反冲成功')
    else:
        print('反冲失败')
        print(_after - _before)
    
    
    
    
    
def excute():
    global table_name
    table_name = 'ky_teswechat_wd_mapping'
    sql_func = 'func_kyfmmajor_ky_teswechat_wd_mapping_zd_inc'
    table_structure = f"""select
des.description as 字段中文名,
--col.table_schema,
--col.table_name,
--col.ordinal_position,
upper(col.column_name) as 字段英文名,
case when col.data_type = 'integer' and upper(col.column_name)='PID' then 'SERIAL'
     when col.data_type = 'integer' then UPPER(col.data_type)
     when col.data_type = 'character varying' then 'VARCHAR('||col.character_maximum_length||')'
     when col.data_type = 'date' then UPPER(col.data_type)
     when col.data_type = 'numeric' then 'NUMERIC('||col.numeric_precision||','||col.numeric_scale||')'
     when col.data_type = 'timestamp without time zone' then 'TIMESTAMP' else col.data_type end as 字段类型,
--col.character_maximum_length,
--col.numeric_precision,
--col.numeric_scale,
case when constraint_name ~ 'uk' then 'UK'
     when constraint_name ~ 'pk' then 'PK'
ELSE NULL end as 是否主键,
case when col.is_nullable = 'NO' THEN 'N'
else null end as 是否可空,
case when col.column_default ~ 'nextval' then null
when col.column_default ~ 'PYXIS' then 'PYXIS'
when col.column_default ~ 'SYSTEM' then '''SYSTEM'''|| ' ::varchar'
else upper(col.column_default) end as 默认值
--des.description
from
information_schema.columns col 
left join pg_description des 
on col.table_name::regclass = des.objoid
and col.ordinal_position = des.objsubid
left join information_schema.key_column_usage USAGE
on col.table_name = USAGE.table_name
and col.column_name = USAGE.column_name
where
col.table_schema = 'public'
and col.table_name = '{table_name}'
order by
col.ordinal_position;"""
    table_structure = get_data(engine,table_structure)
    insert = insert_data(table_name,table_structure)
    if insert:
        backflush(table_name,sql_func)
    
    
def run():
    
    global engine
    if environ_type == 'local':
        engine = initial_engine(_database,'kyfm')
    elif environ_type == 'pyxis':
        engine = 'pyxis_kyfmopr' 
        
    excute()
    
    
run()