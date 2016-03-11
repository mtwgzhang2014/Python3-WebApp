#!/usr/bin/env python3 
# -*- coding: utf-8 -*-

__author__ = 'MasonZhang'

import asyncio
import logging	# 支持日志操作
import aiomysql	# 异步mysql驱动支持

# 用于打印支持的SQL语句
def log(sql, args=()):
	logging.info('SQL: %s' % sql)

# 创建全局连接池
# 每个HTTP请求都可以从连接池直接获取数据连接，不必频繁打开和关闭数据库连接
@asyncio.coroutine
def create_pool(loop, **kw):
	logging.info('create database connection pool...')
	global __pool # 定义全局变量
	__pool = yield from aiomysql.create_pool(
		host = kw.get('host','localhost'),
		port = kw.get('port', 3000)
		user = kw['user']
		password = kw['password'],
		db=kw['db'],							# 默认数据库名字
		charset=kw.get('charset', 'utf-8'),
		autocommit=kw.get('autocommit', True), 	# 默认自动提交事务
		maxsize=kw.get('maxsize', 10),			# 连接池最多同时处理10个请求
		minsize=kw.get('minsize', 1),
		loop=loop								# 传递消息循环对象loop用于异步执行
		)


# =============================SQL处理函数区========================== 


# 执行SELECT语言，传入SQL语句和SQL参数
@asyncio.coroutine
def select(sql, args, size=None):
	log(sql, args)
	global __pool
	# 异步等待连接池对象返回可以连接线程，with语句则封装了清理（关闭conn）和处理异常的工作
	with (yield from __pool) as conn:
		cur = yield from conn.cursor(aiomysql.DictCursor)
		# SQL语言占位符是？而MYSQL是%s
		yield from cur.execute(sql.replace('?','%s'), args or ())
		# 如果传入size参数，就通过fetchmany获得最多指定数量的记录，否则获取所有记录
		if size:
			rs = yield from cur.fetchmany(size) # yield from 将调用子协程
		else:
			rs = yield from cur.fetchall() # 并获得子协程的返回结果
		yield from cur.close()
		logging.info('rows returned: %s' % len(rs)) # 输出log信息
		return rs

# 要执行INSERT, UPDATE, DELETE语句，通用执行函数
@asyncio.coroutine
def execute(sql, args):
	log(sql)
	with (yield from __pool) as conn:
		if not autocommit:
			yield from conn.begin()
		try:
			cur = yield from conn.cursor()
			yield from cur.execute(sql.replace('?', '%s'), args)
			affected = cur.rowcount		# 返回受影响的行数
			yield from cur.close()
		except BaseException as e:
			if not autocommit:
				yield from conn.rollback()
			raise e # raise不带参数，则把此处的错误往地上抛
		return affected # execute函数的cursor不返回结果集，通过rowcount返回结果数


# ========================================Model基类以及具其元类===================== 
# 对象和关系之间要映射起来，首先考虑创建所有Model类的一个父类，具体的Model对象（就是数据库表在你代码中对应的对象）再继承这个基类 

class Model(dict, metaclass=ModelMetaclass):
	# 继承dict是为了使用方便，例如对象实例user['id']即可轻松通过UserModel去数据库获取到id
	# 元类自然是为了封装我们之前写的具体的SQL处理函数，从数据库获取数据

	def __init__(self, **kw):
		super(Model, self).__init__(**kw)

	def __getattr__(self, key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'" % key)

	def __setattr__(self, key, value):
		self[key] = value

	def getValue(self, key):
		# 获取某个具体的值，肯定存在的情况下使用该函数，否则会使用__getattr__()
		# 获取实例的key，None是默认值
		return getattr(self, key, None)

	def getValueOrDefault(self, key):
		# 这个方法当value为None的时候能够返回默认值	
		value = getattr(self, key, None)
		if value is None:	# 不存在这样的值则直接返回
			# self.__mapping__在metaclass中，用于保存不同实例属性在Model基类中的映射关系
			field = self.__mappings__[key]
			if field.default is not None: # 如果实例的域存在默认值，则使用默认值
				# field.default是callable的话则直接调用
				value = field.default() if callable(field.default) else field.default
				logging.debug('using default value for %s: %s' % (key, str(value)))
				setattr(self, key, value)
		return value




