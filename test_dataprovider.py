#!/usr/bin/env python
#-*- coding: utf-8 -*-

from kuanke.data_provider import *
from datetime import datetime

def test_minute():
    security = '000001.XSHE'
    start_dt = datetime(2016, 6, 21, 10, 00)
    end_dt = datetime(2016, 6, 21, 14, 00)
    fields = None
    unit = '1m'
    stock_data_provider = StockDataProvider()
    ret = stock_data_provider.get_bars(security, start_dt, end_dt, unit, fields, skip_paused = False, fq = None)

    print ret

def test_minute1():
    # 601166.XSHG兴业银行在2016-07-25 到 2016-07-29 停牌
    # 测试数据1  start_dt 在2016-07-25前 end_dt在2016－07-29之后
    # 测试程序对停牌时间的数据填充是否正确
    security = '601166.XSHG'
    start_dt = datetime(2016,7,24,10,00)
    end_dt = datetime(2016,7,30,16,00)
    fields = None
    unit = '1m'
    stock_data_provider = StockDataProvider()
    ret = stock_data_provider.get_bars(security, start_dt, end_dt, unit, field_list=['open', 'close', 'volume', 'money'], skip_paused = False, fq = None) 
    print ret

    # 测试数据2 start_dt 在2016-07-25前 end_dt为2016-07-28(停牌时间)
    # 测试程序对结束日期恰好停牌的处理
    end_dt = datetime(2016,7,28,14,00)
    ret = stock_data_provider.get_bars(security, start_dt, end_dt, unit, field_list=['open', 'close', 'volume', 'money'], skip_paused = False, fq = None)
    print ret

    # 测试数据3 start_dt 为2016-07-26(停牌时间) end_dt在2016－07-29之后
    # 测试程序对起始日期恰好停牌的处理
    start_dt = datetime(2016,7,26,9,40)
    end_dt = datetime(2016,7,30,16,00)
    ret = stock_data_provider.get_bars(security, start_dt, end_dt, unit, field_list=['open', 'close', 'volume', 'money'], skip_paused = False, fq = None)
    #print ret

    # 测试程序4 start_dt 为2016-07-24 end_dt 为2016-07-30
    # 测试程序对默认输入的支持性 和 非交易时段输入的处理
    start_dt = datetime(2016,7,24)
    end_dt = datetime(2016,7,30)
    ret = stock_data_provider.get_bars(security, start_dt, end_dt, unit, field_list=['open', 'close', 'volume', 'money'], skip_paused = False, fq = None)
    print ret

    
def test_day():
    security = '002642.XSHE'
    start_dt = datetime(2016, 6, 21)
    end_dt = datetime(2016, 7, 21)
    fields = None
    unit = '1d'
    stock_data_provider = StockDataProvider()
    ret = stock_data_provider.get_bars(security, start_dt, end_dt, unit, fields, skip_paused = False, fq = 'pre')
    print ret