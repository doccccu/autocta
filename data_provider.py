#!/usr/bin/env python

import bcolz
import numpy as np
import pandas as pd
import os

class StockDataProvider():
	def __init__(self):
		self.__rootdir_day = '/data/bcolz/daydata/'
		self.__rootdir_minute = '/data/bcolz/minutedata/'
		self.__day_columns = 'open close high low volume money pre_close high_limit low_limit paused avg factor'.split()
		self.__minute_columns = 'open close high low volume money avg'.split()

	def __get_index(security, dt, unit):
		if unit == '1d':
			return self.__get_day_index(security, dt)
		elif unit == '1m':
			return self.__get_minute_index(security, dt)

	def __get_day_index(security, dt):
		#如果找不到bcolz数据文件 说明所查询的股票没有数据文件		
		rootdir = __rootdir_day + security
		if not os.path.exists(rootdir+'__rootdirs__'):
			raise Exception("获取股票天数据失败,文件不存在")

		#通过bcolz文件得到只有日期的ret(np array类型)
		ct = bcolz.open(rootdir, mode='r')['date']
		ret = ct[:]

		#如果所查询日期超出了ret中日期的范围 说明查询日期超上限
		dt_int = dt.year * 10000 + dt.month * 100 + dt.day
		if dt_int < ret[0] or dt_int > ret[-1]:
			raise Exception("查询日期超出数据库范围")

		return ret.searchsorted(np.float64(dt_int))

	def __get_minute_index(security, dt):
		#如果找不到bcolz数据文件 说明所查询的股票没有数据文件
		rootdir = __rootdir_minute + security
		if not os.path.exists(rootdir+'__rootdirs__'):
			raise Exception("获取股票分钟数据失败,文件不存在")
		#获得天数索引
		day_index = __get_day_index(security, dt)

		#建立每天小时对应已经开盘多少分钟的映射
		hour_to_minute = {9:-30, 10:30, 11:90, 12: 120, 13:120, 14:180, 15:180}
		#判断查询小时和分钟是否超出范围
		dt_int = dt.hour * 100 + dt.minute
		if dt_int < 930 or dt_int > 1500:
			raise Exception("查询时间超出交易时间")

		ret = day_index * 240 + hour_to_minute[dt.hour] + dt.minute

		return ret

	def get_bars(security, start_dt, end_dt, unit, field_list = None):
		if unit == '1d':	
			return get_day_bar(security, start_dt, end_dt, field_list)
		elif unit == '1m':
			return get_minute_bar(security, start_dt, end_dt, field_list)

	def get_day_bar(security, start_dt, end_dt, field_list = None):
		
		start_index = __get_index(security, start_dt)
		end_index = __get_index(security, end_dt)
		#通过start_index算出end_index
		#end_index = end_dt.day - start_dt.day + start_index

		rootdir = __rootdir_day + security
		ct = bcolz.open(rootdir, mode = 'r')[start_index:end_index+1]	

		if field_list == None:
			field_list = self.__day_columns

		#建立一个空的np array来存储查询结果
		dtype_field = np.dtype([(field, ct.dtype[field]) for field in field_list])
		ret = np.empty(np.shape(end_index + 1 - start_index, ), dtype = dtype_field)
		for field in field_list:
			ret[field] = ct[field][:]

		return ret

	def get_minute_bar(security, start_dt, end_dt, field_list = None):
		start_index = __get_index(security, start_dt)
		end_index = __get_index(security, end_dt)
	
		rootdir = __rootdir_minute + security
		ct = bcolz.open(rootdir, mode = 'r')[start_index:end_index + 1]

		if field_list == None:
			field_list = self.__minute_columns

		#建立一个空的np array来存储查询结果
		dtype_field = np.dtype([(filed, ct.dtype[field]) for field in field_list])
		ret = np.empty(np.shape(end_index + 1 - start_index, ), dtype = dtype_field)
		for field in field_list:
			ret[field] = ct[field][:]

		return ret





