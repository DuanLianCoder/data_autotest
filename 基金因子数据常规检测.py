# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 17:30:56 2019

@author: EX-DUANLIAN002
"""


from sqlalchemy import create_engine
import pandas as pd
from datetime import timedelta,datetime
import datetime
import calendar
import logging
from pandas.tseries.offsets import MonthEnd, QuarterEnd, YearEnd

_database = {'d0qer':{'password':'Paic2018','port':7616,'ip':'10.20.131.60','name':'d0qer','user':'dlmopr'},
             "kyfm":{"password":"Paic2019","port":7658,"ip":"30.31.0.182","name":"kyfm","user":"kyfmdata"}}

class Initial_Engine():
    """封装数据库的相关操作
       ---初始化
       ---pandas读取数据库信息
       ---engine执行原生sql语句
    """
    def __init__(self,env=None):
        if env == None:
            self.engine = 'pyxis_kyfmopr'
        else:
            self.engine = create_engine('postgresql+psycopg2://{user}:{password}@{ip}:{port}/{name}'.format(**env))
        
    def read_sql_query(self,sql=None):
        if self.engine == 'pyxis_kyfmopr':
            data_result = query_db(sql,self.engine)
        else:
            data_result = pd.read_sql_query(sql,self.engine)
        return data_result
    
    def sql_execute(self,sql):
        if self.engine == 'pyxis_kyfmopr':
            return db_execute(sql, data_source_name = self.engine)
        else:
            return self.engine.execute(sql)
        
self = Initial_Engine(_database['kyfm'])
        
class Check_Factor(Initial_Engine):
    def __init__(self,env=None):
        super().__init__(env)
        self.result = pd.DataFrame(columns=['factor_id','data_frequency','factor_date_max'])
        self.newest_date = {}
 
     #获取当前日期最新因子应该落地的时间
    def set_current(self):
         now = datetime.date.today()
         offset_m, offset_q = MonthEnd(), QuarterEnd()
         self.newest_date['M'] = offset_m.rollback(now) 
         self.newest_date['Q'] = offset_q.rollback(now)
         self.newest_date['D'] = now - timedelta(days=1)
         self.newest_date['Y'] = YearEnd().rollback(now)
         half1 = datetime.date(now.year, 6, 30)
         half2 = datetime.date(now.year, 12, 31)
         if now < half1:
             self.newest_date['H'] = datetime.date(now.year-1, 12, 31)
         elif now < half2:
             self.newest_date['H'] = half1
         else:
             self.newest_date['H'] = half2
         
    #获取一个因子的数据和对应的频率     
    def get_factor_data(self, factor_id):
        #factor_id = 'ZF001310'
        query_sql = "select table_result,data_frequency from ky_fund_factor_info_all where factor_id = '{0}'".format(factor_id)       
        table_name_temp = list(self.sql_execute(query_sql))
        table_name = table_name_temp[0][0]
        frequency = table_name_temp[0][1]
        for_data = """select factor_id,object_code,factor_date,factor_value,effective_date,is_newest,is_effective_newest,DECLARE_date,is_valid 
                      from {} where factor_id = '{}'
                      ORDER BY factor_id,object_code,factor_date""".format(table_name, factor_id)
        data = self.read_sql_query(for_data)
        return data, frequency
    
    #检测is_newest字段
    def is_newest_examine(self, data):
        is_max = data.groupby(['factor_id','object_code']).factor_date.apply(lambda t:t == t.max()).apply(lambda x:1 if x else 0)
        data['is_newest_expect'] = is_max
        result = data['is_newest'] == data['is_newest_expect']
        if False in result.values:
            print('is_newest字段value存在问题')
            print(data[data['is_newest'] != data['is_newest_expect']])
        else:
            print('is_newest字段检测通过')
            
    #检测is_effective_newest字段
    def is_effective_newest_examine(self, data):
        is_max = data.groupby(['factor_id','object_code','factor_date']
                 ).effective_date.apply(lambda t:t == t.max()).apply(lambda x:1 if x else 0)
        result = data['is_effective_newest'] == is_max
        if False in result.values:
            logging.warning('is_effective_newest字段存在问题')
        else:
            print('is_effective_newest字段检测通过')

    #检测declare_date字段
    def declare_date_examine(self, data):
        result = data.factor_date == data.declare_date
        if False in result.values:
            logging.warning('declare_date字段存在问题')
        else:
            print('declare_date字段检测通过')
            
    #检测factor_date是否满足标准
    def factor_date_examine(self, data, frequency):
        if frequency == 'D':
            factor_diff = data.groupby(['factor_id','object_code']).factor_date.diff()
            compare = factor_diff == timedelta(days=1)
            if any(compare):
                print('该因子符合日频标准')
            else:
                logging.warning('该因子不符合日频标准')
                print()
                
        elif frequency == 'W':
            factor_diff = data.groupby(['factor_id','object_code']).factor_date.diff()
            if timedelta(days=7) in factor_diff.values:
                print('该因子符合周频标准')
            else:
                logging.warning('该因子不符合周频标准')
                
        elif frequency == 'M':
            #计算每个月的月末
            expect_day = data.factor_date.apply(lambda x:MonthEnd().rollforward(x))
            #比较factor_date与预期月末是否相符
            result = data.factor_date.astype('datetime64[ns]') == expect_day
            if False in result.values:
                logging.warning('存在不是月末的factor_date')
            else:
                print('factor_date是每个月的月末')
                
            #计算月频是否间隔一个月   
            data['data_month'] = data['factor_date'].apply(lambda x:x.month) 
            data['data_year'] = data['factor_date'].apply(lambda x:x.year) 
            diff = data.groupby(['object_code'])['data_month','data_year'].diff().apply(lambda x:x.data_year*12+x.data_month,axis=1)
            result = diff == 1
            if all(result):
                print('月频间隔符合一个月')
            else:
                print('存在月频间隔不符合一个月')
                
        elif frequency == 'Q':
            #截取factor_date的day
            date_q = ['0331','0630','0930','1231']
            factor_date_end = data['factor_date'].apply(lambda x:datetime.datetime.strftime(x,'%m%d'))
            expect_compare = factor_date_end.isin(date_q)
            
            if all(expect_compare):
                print('日期符合正常季末')
            else:
                logging.warning('非正常季末')
                
            #计算季频是否间隔三个月   
            data['data_month'] = data['factor_date'].apply(lambda x:x.month) 
            data['data_year'] = data['factor_date'].apply(lambda x:x.year) 
            diff = data.groupby(['object_code'])['data_month','data_year'].diff().apply(lambda x:x.data_year*12+x.data_month,axis=1)
            result = diff.dropna() == 3
            if all(result):
                print('季频间隔符合一个季度')
            else:
                print('存在季频间隔不符合三个月')
                print(diff)
                
        elif frequency == 'Y':
            #截取factor_date的day
            date_q = ['1231']
            factor_date_end = data['factor_date'].apply(lambda x:datetime.datetime.strftime(x,'%m%d'))
            expect_compare = factor_date_end.isin(date_q)
            
            if all(expect_compare):
                print('日期符合正常年末')
            else:
                logging.warning('非正常年末')
                
            data['data_year'] = data['factor_date'].apply(lambda x:x.year)    
            result = data.groupby(['object_code']).data_year.diff() == 1
            if all(result):
                print('年频间隔符合一年')
            else:
                print('存在年频间隔不符合十二个月')           

        elif frequency == 'H':
            #截取factor_date的day
            date_q = ['0630', '1231']
            factor_date_end = data['factor_date'].apply(lambda x:datetime.datetime.strftime(x,'%m%d'))
            expect_compare = factor_date_end.isin(date_q)
            
            if all(expect_compare):
                print('日期符合正常半年频')
            else:
                logging.warning('非正常半年频')

            #计算季频是否间隔三个月   
            data['data_month'] = data['factor_date'].apply(lambda x:x.month) 
            data['data_year'] = data['factor_date'].apply(lambda x:x.year) 
            diff = data.groupby(['object_code'])['data_month','data_year'].diff().apply(lambda x:x.data_year*12+x.data_month,axis=1)
            result = diff == 6
            if all(result):
                print('半年频间隔符合6个月')
            else:
                print('存在半年频间隔不符合6个月')  
                
    #检测declare_date字段
    def is_factor_date_newsest_examine(self, data, frequency):
        result = pd.to_datetime(data.factor_date.max())
        if self.newest_date[frequency] == result:
            print('最新日期正常')
        elif result < self.newest_date[frequency]:
            print('因子数据未到最新')
        else:
            print('因子数据超前')

    
    def get_result(self,factors):
        #初始化当前日期的最新月末季末
        self.set_current()
        for factor in factors:
            print('{}因子开始检测........'.format(factor))
            data, frequency = self.get_factor_data(factor)
            self.is_newest_examine(data)
            self.is_effective_newest_examine(data)
            self.declare_date_examine(data)
            self.factor_date_examine(data, frequency)
            self.is_factor_date_newsest_examine(data, frequency)
            
            
            
def run():
    factors = ['C004780','C004781','C004782','C004783','C004784','C004785','C004786','C004787','C004788','C004789','C004790','C004791','C004792','C004793','C004794','C004795','C004796','C004797','C004798','C004799','C004800','C004801','C004802','C004803','C004804','C004805','C004806','C004807','C004808','C004809','C004810','C004811','C004812','C004813','C004814','C004815','C004816','C004817','C004818','C004819','C004820','C004821','C004822','C004823','C004824','C004825']
    check_Factor = Check_Factor(_database['kyfm'])
    check_Factor.get_result(factors)
    
    
run()
    
