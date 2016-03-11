#!/usr/bin/env python3 
# -*- coding: utf-8 -*-

__author__ = 'MasonZhang'

import asyncio, logging

import aiomysql


@asyncio.coroutine
def create_pool(loop, **kw):
	logging.info('create database connection pool...')
	global __pool
	__pool = yield from aiomysql.create_pool(
		host = kw.get('host','localhost'),
		port = kw.get('port', 3000)
		user = kw['user']
		password = kw['password'],
		db=kw['db'],
		charset=kw.get('charset', 'utf-8'),
		autocommit=kw.get('autocommit', True),
		maxsize=kw.get('maxsize', 10),
		minsize=kw.get('minsize', 1),
		loop=loop
		)

