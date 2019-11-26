# -*- coding: utf-8 -*-
"""
Created on Mon Aug  5 11:36:10 2019

@author: EX-DUANLIAN002
"""

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import time

_database = {'d0qer':{'password':'Paic2018','port':7616,'ip':'10.20.131.60','name':'d0qer','user':'dlmopr'},
             "kyfm":{"password":"Paic2019","port":7658,"ip":"30.31.0.182","name":"kyfm","user":"kyfmdata"}}

class Initial_Engine():
    """封装数据库的相关操作
       ---初始化
       ---pandas读取数据库信息
       ---engine执行原生sql语句
    """
    def __init__(self, env=None):
        if env == None:
            self.engine = 'pyxis_kyfmopr'
        else:
            self.engine = create_engine('postgresql+psycopg2://{user}:{password}@{ip}:{port}/{name}'.format(**env))
        
    def read_sql_query(self, sql=None):
        if self.engine == 'pyxis_kyfmopr':
            data_result = query_db(sql, self.engine)
        else:
            data_result = pd.read_sql_query(sql, self.engine)
        return data_result
    
    def sql_execute(self, sql):
        if self.engine == 'pyxis_kyfmopr':
            return db_execute(sql, data_source_name = self.engine)
        else:
            return self.engine.execute(sql)
        
class Examine_table_structure(Initial_Engine):
    """比对业务文档与数据库建表结构是否一致
       ---env:数据库环境配置
    """
    def __init__(self, env=None):
        super().__init__(env)
        
    #读取业务文档表结构
    def get_business_structures(self, path):
        self.business_structures = pd.read_excel(path, header=None)
        self.business_structures.replace(np.nan, 0, inplace=True)

    #获取业务文档指定单个表结构    
    def get_business_structure(self, table):
        #定位table在文档中的开始行索引位置
        start = self.business_structures[self.business_structures[1] == table].index[0]+1
        #定位结束位置,第一次出现0的位置
        end = start
        while end < self.business_structures.shape[0] and self.business_structures[1][end]:
            end += 1
        #清洗  
        business_structure = self.business_structures.iloc[start:end]
        #print(business_structure)
        business_structure.columns = business_structure.iloc[0]
        business_structure = business_structure.drop([start])
        business_structure.reset_index(drop=True, inplace= True)
        return business_structure
    
    #获取数据库表结构
    def get_database_structure(self, table):
        #sql直接获取数据库表结构
        base_sql = """select
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
             when col.data_type = 'timestamp without time zone' then 'TIMESTAMP'
             when col.data_type = 'text' then 'TEXT' else col.data_type end as 字段类型 ,
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
        and col.table_name = '{0}'
        order by
        col.ordinal_position;""".format(table)
                    
        database_structure = self.read_sql_query(base_sql)
        database_structure = database_structure.fillna(0)
        return database_structure
    
    #对所有表结构进行对比
    def compare_structure(self, tables):
        for table in tables:
            #文档表结构
            business_structure = self.get_business_structure(table)
            #数据库表结构
            database_structure = self.get_database_structure(table)
            #print(business_structure, database_structure)
            #比较
            result = business_structure==database_structure
            if result.values.all():
                print('{0}表结构正确'.format(table))
            else:
                print('{0}表结构有误'.format(table))
                print(result)
                break
            
def run():
    tables = ['ky_fund_perf_rar_ir_ir5_app',
'ky_fund_perf_rar_ir_ir6_app',
'ky_fund_perf_rar_ir_ir1_app',
'ky_fund_perf_rar_ir_ir2_app',
'ky_fund_perf_rar_ir_ir3_app',
'ky_fund_perf_rar_ir_ir4_app',
'ky_zfund_perf_rar_ir_ir5_app',
'ky_zfund_perf_rar_ir_ir6_app',
'ky_zfund_perf_rar_ir_ir1_app',
'ky_zfund_perf_rar_ir_ir2_app',
'ky_zfund_perf_rar_ir_ir3_app',
'ky_zfund_perf_rar_ir_ir4_app',
'ky_pfund_perf_rar_ir_ir1_app',
'ky_pfund_perf_rar_ir_ir6_app',
'ky_pfund_perf_rar_ir_ir5_app',
'ky_pfund_perf_rar_ir_ir3_app',
'ky_pfund_perf_rar_ir_ir4_app',
'ky_pfund_perf_rar_ir_ir2_app',
'ky_fund_perf_rar_ir_ir7_app',
]
    examine_table_structure = Examine_table_structure(_database['kyfm'])
    #读取本地业务文档表结构
    examine_table_structure.get_business_structures('business_structures.xlsx')
    examine_table_structure.compare_structure(tables)
    
    
run()
    