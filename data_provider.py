#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import bcolz
import numpy as np
import pandas as pd
import os
from datetime import *
import json

def date_to_npf64(date):
	return np.float64(date.year*10000 + date.month*100 + date.day)

def round_2(num):
	return np.round(num, 2)


class StockDataProvider():
	def __init__(self):
		self.__rootdir_day = '/data/bcolz/daydata/'
		self.__rootdir_minute = '/data/bcolz/minutedata/'
		self.__day_columns = 'open close high low volume money pre_close high_limit low_limit paused avg factor date'.split()
		self.__minute_columns = 'open close high low volume money avg factor'.split()
		self.__day_none_columns = 'open close high low volume money'.split()
		self.__trade_minute = self.__get_trade_minute()
		self.__trade_day = self.__get_all_trade_datetime()

	def __get_day_index(self, security, dt):
		# 如果找不到bcolz数据文件 说明所查询的股票没有数据文件
		rootdir = self.__rootdir_day + security
		if not os.path.exists(rootdir+'/__rootdirs__'):
			raise Exception("获取股票天数据失败,文件不存在")

		# 通过bcolz文件得到只有日期的ret(np array类型)
		ct = bcolz.open(rootdir, mode='r')['date']
		ret = ct[:]

		# 如果所查询日期超出了ret中日期的范围 说明查询日期超上限
		dt_int = dt.year * 10000 + dt.month * 100 + dt.day
		if dt_int < ret[0] or dt_int > ret[-1]:
			raise Exception("查询日期超出数据库范围")

		return ret.searchsorted(np.float64(dt_int), side = 'right') - 1

	def __get_minute_index(self, security, dt):
		# 如果找不到bcolz数据文件 说明所查询的股票没有数据文件
		rootdir = self.__rootdir_minute + security
		if not os.path.exists(rootdir+'/__rootdirs__'):
			raise Exception("获取股票分钟数据失败,文件不存在")
		# 获得天数索引
		day_index = self.__get_day_index(security, dt)

		# 建立每天小时对应已经开盘多少分钟的映射
		hour_to_minute = {9:-30, 10:30, 11:90, 12: 120, 13:120, 14:180, 15:180}

		ret = day_index * 240 + hour_to_minute[dt.hour] + dt.minute

		return ret

	def __get_trade_minute(self):
		a = datetime(2017,1,1,9,30)
		ret = []
		while (1):
			tmp = a.hour * 100 + a.minute
			a = a + timedelta(minutes=1)
			if tmp <= 1129:
				ret.append(tmp)
			if tmp > 1129:
				break
		a = datetime(2017,1,1,13,00)
		while (1):
			tmp = a.hour * 100 + a.minute
			a = a + timedelta(minutes=1)
			if tmp <=1459:
				ret.append(tmp)
			if tmp > 1459:
				break
		return np.array(ret)

	def __get_minute_index_start(self, security, start_dt, end_dt):
		rootdir = self.__rootdir_minute + security
		if not os.path.exists(rootdir+'/__rootdirs__'):
			raise Exception("获取股票分钟数据失败,文件不存在")

		rootdir_day = self.__rootdir_day + security
		ct = bcolz.open(rootdir_day, mode='r')['date']
		ret = ct[:]

		trade_minute = np.array(self.__get_trade_minute())

		start_dt_int = np.float64(start_dt.year * 10000 + start_dt.month * 100 + start_dt.day)
		day_start_index = ret.searchsorted(start_dt_int)
		delta_start = 0
		if  start_dt_int == ret[day_start_index]:
			delta_start = trade_minute.searchsorted(start_dt.hour*100+start_dt.minute)
		start_index = day_start_index * 240 + delta_start

		end_dt_int = np.float64(end_dt.year * 10000 + end_dt.month * 100 + end_dt.day)
		day_end_index = ret.searchsorted(end_dt_int, side = 'right')
		delta_end = 0
		if end_dt_int == ret[day_end_index - 1]:
			delta_end = 240 - trade_minute.searchsorted(end_dt.hour*100+end_dt.minute, side='right')
		end_index = day_end_index * 240 - delta_end

		return start_index, end_index

	def __get_index(self, security, dt, unit):
		if unit == '1d':
			return self.__get_day_index(security, dt)
		elif unit == '1m':
			return self.__get_minute_index(security, dt)

	def __get_all_trade_day(self):
		# rootdir_json = "/home/dounaifu/kuanke/jqdata/data/all_trade_days.json"
		rootdir_json = os.path.dirname(os.getcwd()) + '/jqdata/data/all_trade_days.json'
		with open(rootdir_json, mode='r') as json_file:
			days = json.load(json_file)
			days = [ datetime.strptime(d, '%Y-%m-%d') for d in days ]
			days = [ np.float64((d.year*10000 + d.month*100 + d.day)) for d in days]
			days_np = np.array(days)
			return days_np

	def __get_all_trade_datetime(self):
		# rootdir_json = "/home/dounaifu/kuanke/jqdata/data/all_trade_days.json"
		rootdir_json = os.path.dirname(os.getcwd()) + '/jqdata/data/all_trade_days.json'
		with open(rootdir_json, mode='r') as json_file:
			days = json.load(json_file)
			days = [ datetime.strptime(d, '%Y-%m-%d') for d in days ]
			return days

    # 处理天数据停牌
	def __handle_skip_paused_day(self, ret, start_dt, end_dt):
		trade_days = self.__get_all_trade_day()

		start_dt = date_to_npf64(start_dt)
		end_dt = date_to_npf64(end_dt)

		start_index = trade_days.searchsorted(start_dt, side = 'right') - 1
		end_index = trade_days.searchsorted(end_dt, side = 'right') - 1
		trade_days = trade_days[start_index:end_index + 1]

		#对新的dataframe进行填充
		ret_raw = pd.DataFrame(ret)
		ret_raw = ret_raw.set_index('date')
		ret_all = ret_raw.reindex(trade_days)
		to_fill = ret_all.loc[~(ret_all.volume > 0)]
		#volume和money添0   pasued添1
		fill_zero = to_fill.loc[:, ['volume', 'money']]
		fill_one = to_fill.loc[:, ['paused']]
		ret_all.update(fill_zero.fillna(0))
		ret_all.update(fill_one.fillna(1))
		#先填充close列 再横向填充
		ret_all['close'].fillna(method = 'pad', inplace = True)
		for d in self.__day_columns:
			if d != 'date':
				ret_all[d].fillna(ret_all['close'], inplace = True)
		ret = np.array(ret_all.to_records())

		return ret

	# 处理天数据复权
	def __handle_fq_day(self, security, ret, fq, benchmark_day):
		fq_columns = 'open close high low pre_close high_limit low_limit avg'.split()
		benchmark_index = self.__get_index(security, benchmark_day, '1d')
		rootdir = self.__rootdir_day + security
		benchmark_factor = bcolz.open(rootdir, mode='r')['factor'][benchmark_index]

		ret_raw = pd.DataFrame(ret)
		# 不复权处理
		if fq == None:
			#factor = 1
			ret_raw['factor'] = np.float64(1.0)
		# 前复权处理
		if fq == 'pre':
			for colz in fq_columns:
				ret_raw[colz] = ret_raw[colz] * ret_raw['factor'] / benchmark_factor
				ret_raw[colz] = ret_raw[colz].apply(round_2)
			ret_raw['volume'] = ret_raw['volume'] / ret_raw['factor'] * benchmark_factor
			ret_raw['volume'] = ret_raw['volume'].apply(np.round)
			ret_raw['factor'] = ret_raw['factor'] / benchmark_factor
			# ret_raw['factor'] = ret_raw['factor'].apply(round_2)

		# 后复权处理
		if fq == 'post':
			for colz in fq_columns:
				ret_raw[colz] = ret_raw[colz] * ret_raw['factor']
				ret_raw[colz] = ret_raw[colz].apply(round_2)
			ret_raw['volume'] = ret_raw['volume'] / ret_raw['factor']
			ret_raw['volume'] = ret_raw['volume'].apply(np.round)

		return np.array(ret_raw.to_records())	

	def __deal_trade_hour(self, tmp):
		hour_minute = tmp.hour*100 + tmp.minute
		if hour_minute < 930:
			return datetime(tmp.year, tmp.month, tmp.day, 9, 30)
		if hour_minute > 1500:
			return datetime(tmp.year, tmp.month, tmp.day, 15, 00)
		return tmp

	def __insert_paused_minute(self, security, ret, insert_index, insert_dt, insert_row_num):
		# 找到最近的收盘价格用于填充数据
		# 获得pre_close pre_factor
		rootdir = self.__rootdir_day + security
		ct = bcolz.open(rootdir, mode='r')['date'][:]
		day_index = ct.searchsorted(date_to_npf64(insert_dt), side='right') - 1
		pre_close = bcolz.open(rootdir, mode='r')['close'][day_index]
		pre_factor = bcolz.open(rootdir, mode='r')['factor'][day_index]

		# 根据ret的列  pre_close factor  insert_dt 生成一天的dataframe
		# 计算有多少行
		#trade_minute = np.array(self.__get_trade_minute())
		#insert_row_num = 240 - trade_minute.searchsorted(insert_dt.hour*100 + insert_dt.minute)
		# 生成空的dataframe
		col_list = list(ret.columns)
		tmp_np = np.zeros(shape = insert_row_num)
		insert_pd = pd.DataFrame(tmp_np)
		# volume和money填0 factor填pre_factor 其他填pre_close
		# 分钟数据没有paused列
		for one in col_list:
			if one == 'volume' or one == 'money':
				insert_pd[one] = 0
			elif one == 'factor':
				insert_pd[onw] = pre_factor
			else:
				insert_pd[one] = pre_close
		insert_pd.drop([0], axis = 1, inplace = True)

		# print insert_row_num

		# 将生成的dataframe插入ret中
		if insert_index == 0:
			ret = insert_pd.append(ret, ignore_index = True)
		elif insert_index == len(ret):
			ret = ret.append(insert_pd, ignore_index = True)
		else:
			tmp_df = ret[0:insert_index-1].append(insert_pd, ignore_index = True)
			ret = tmp_df.append(ret[insert_index:], ignore_index = True)
		return ret

	# 处理分钟数据停牌
	def __handle_skip_paused_minute(self, security, ret, start_dt, end_dt):
		trade_days = np.array(self.__trade_day)
		start_dt_int = date_to_npf64(start_dt)
		end_dt_int = date_to_npf64(end_dt)

		# 获得从起始到结束包含停牌日期的所有交易日期
		start_index = trade_days.searchsorted(start_dt)
		end_index = trade_days.searchsorted(end_dt)
		trade_days_all = trade_days[start_index:end_index]
		# 获得从起始到结束  不  包含停牌日期的所有交易日期
		rootdir_day = self.__rootdir_day + security
		trade_days_raw = bcolz.open(rootdir_day, mode='r')['date'][:]
		start_index_raw = trade_days_raw.searchsorted(start_dt_int)
		end_index_raw = trade_days_raw.searchsorted(end_dt_int)
		trade_days_raw = trade_days_raw[start_index_raw:end_index_raw]

		# 以天为单位向原始数据中插入停牌数据
		ret = pd.DataFrame(ret)
		trade_minute = np.array(self.__get_trade_minute())
		now_index = 0
		for one in trade_days_all:
			# 没停牌就跳过(没有考虑更高效的方法)
			flag_pause = True
			if one in trade_days_raw:
				flag_pause = False
			# 如果是起始日期 插入数据采用起始日期的小时和分钟
			if date_to_npf64(one) == start_dt_int:
				insert_row_num = 240 - trade_minute.searchsorted(start_dt.hour*100 + start_dt.minute)
				if flag_pause:
					ret = self.__insert_paused_minute(security, ret, now_index, start_dt, insert_row_num)
				now_index += insert_row_num
			# 如果是终止日期 插入数据采用终止日期的小时和分钟
			elif date_to_npf64(one) == end_dt_int:
				insert_row_num = trade_minute.searchsorted(end_dt.hour*100 + end_dt.minute) + 1
				if flag_pause:
					ret = self.__insert_paused_minute(security, ret, now_index, end_dt, insert_row_num)
				now_index += insert_row_num
			# 如果是中间日期 插入数据采用9:30
			else:
				insert_row_num = 240
				if flag_pause:
					insert_dt = datetime(one.year,one.month,one.day,9,30)
					ret = self.__insert_paused_minute(security, ret, now_index, insert_dt, insert_row_num)
				now_index += insert_row_num

		return np.array(ret.to_records())

	def get_day_bar(self, security, start_dt, end_dt, field_list = None, skip_paused = False, fq = None):
		start_index = self.__get_index(security, start_dt, '1d')
		end_index = self.__get_index(security, end_dt, '1d')

		# 通过start_index算出end_index
		# end_index = end_dt.day - start_dt.day + start_index

		rootdir = self.__rootdir_day + security
		ct = bcolz.open(rootdir, mode = 'r')[start_index:end_index + 1]	

		if field_list == None:
			field_list = self.__day_columns

		if 'date' not in field_list:
			field_list.append('date')
		# 建立一个空的np array来存储查询结果
		dtype_field = np.dtype([(field, ct.dtype[field]) for field in field_list])
		ret = np.empty(shape = (end_index + 1 - start_index, ), dtype = dtype_field)
		for field in field_list:
			ret[field][:] = ct[field][:]

		# 先进行复权处理 再进行停牌处理
		now_time = datetime.now()
		ret = self.__handle_fq_day(security, ret, fq, datetime(now_time.year, now_time.month , now_time.day))

		if skip_paused == False:
			ret = self.__handle_skip_paused_day(ret, start_dt, end_dt)


		# 数据范围check
		if date_to_npf64(start_dt) > ret['date'][0]:
			return ret[1:]
		else:
			return ret

	def get_minute_bar(self, security, start_dt, end_dt, field_list = None, skip_paused = False,fq = 'pre'):
		start_dt_trade = self.__deal_trade_hour(start_dt)
		end_dt_trade = self.__deal_trade_hour(end_dt)

		start_index, end_index = self.__get_minute_index_start(security, start_dt_trade, end_dt_trade)
	
		rootdir = self.__rootdir_minute + security
		ct = bcolz.open(rootdir, mode = 'r')[start_index:end_index]

		if field_list == None:
			field_list = self.__minute_columns

		# 建立一个空的np array来存储查询结果
		dtype_field = np.dtype([(field, ct.dtype[field]) for field in field_list])
		ret = np.empty(shape = (end_index - start_index, ), dtype = dtype_field)
		for field in field_list:
			ret[field] = ct[field][:]

		if skip_paused == False:
			ret = self.__handle_skip_paused_minute(security, ret, start_dt_trade, end_dt_trade)

		return ret

	def get_bars(self, security, start_dt, end_dt, unit, field_list = None, skip_paused = False, fq = 'pre'):
		if unit == '1d':	
			return self.get_day_bar(security, start_dt, end_dt, field_list, skip_paused, fq)
		elif unit == '1m':
			return self.get_minute_bar(security, start_dt, end_dt, field_list, skip_paused)




