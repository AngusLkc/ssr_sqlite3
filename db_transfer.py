#!/usr/bin/python
# -*- coding: UTF-8 -*-

import logging
import time
import sys
import sqlite3
from server_pool import ServerPool
import traceback
from shadowsocks import common, shell, lru_cache, obfs

db_instance = None #存储自身实例

class TransferBase(object):
	def __init__(self):
		import threading
		self.event = threading.Event()
		self.last_update_transfer = {} #历史增量之和
		self.force_update_transfer = {} #强制更新表
		self.pull_ok = False

	def push_db_all_user(self):
		if self.pull_ok is False:
			return
		current_transfer=ServerPool.get_instance().get_servers_transfer()
		dt_transfer={}
		for id in self.force_update_transfer:
			if id in self.last_update_transfer:
				if self.force_update_transfer[id][0]+self.force_update_transfer[id][1]>self.last_update_transfer[id][0]+self.last_update_transfer[id][1]:
					dt_transfer[id]=[self.force_update_transfer[id][0]-self.last_update_transfer[id][0], self.force_update_transfer[id][1]-self.last_update_transfer[id][1]]
				del self.last_update_transfer[id]
		for id in current_transfer:
			if id in self.force_update_transfer:
				continue
			if id in self.last_update_transfer:
				if current_transfer[id][0]+current_transfer[id][1]-self.last_update_transfer[id][0]-self.last_update_transfer[id][1]<=0:
					continue
				dt_transfer[id]=[current_transfer[id][0]-self.last_update_transfer[id][0],current_transfer[id][1]-self.last_update_transfer[id][1]]
			else:
				if current_transfer[id][0]+current_transfer[id][1]<=0:
					continue
				dt_transfer[id]=[current_transfer[id][0],current_transfer[id][1]]
		self.update_all_user(dt_transfer)
		for id in dt_transfer:
			if id not in self.force_update_transfer:
				last=self.last_update_transfer.get(id,[0,0])
				self.last_update_transfer[id]=[last[0]+dt_transfer[id][0],last[1]+dt_transfer[id][1]]
		self.force_update_transfer={}

	def del_server_out_of_bound_safe(self, rows):
		cur_servers = {} #存放每次整理参数所有有效端口号
		new_servers = {} #存放由于参数变更或新增的端口号
		cur_running = ServerPool.get_instance().tcp_servers_pool
		for row in rows:
			if row['u'] + row['d'] >= row['quota']: #超流了
				continue
			port = row['port']
			passwd = self.to_bytes(row['passwd'])
			if hasattr(passwd, 'encode'):
				passwd = passwd.encode('utf-8')
			cfg = {'password': passwd}
			read_config_keys = ['method', 'obfs', 'obfs_param', 'protocol', 'protocol_param', 'speed_limit_per_user']
			for param_key in read_config_keys:
				if param_key in row and row[param_key]:
					cfg[param_key] = row[param_key]
			merge_config_keys = ['password'] + read_config_keys
			for name in cfg.keys():
				if hasattr(cfg[name], 'encode'):
					try:
						cfg[name] = cfg[name].encode('utf-8')
					except Exception as e:
						logging.warning('encode cfg key "%s" fail, val "%s"' %(name, cfg[name]))
			if port not in cur_servers:
				cur_servers[port] = passwd
			else:
				logging.error('端口冲突: [%s]' %(port))
				continue
			#处理参数变更和需要新增的服务端口
			if port in cur_running:
				port_param = ServerPool.get_instance().get_config(port)
				for name in merge_config_keys:
					if port_param and cfg[name] and port_param[name] and not self.cmp(cfg[name], port_param[name]):
						self.force_update_transfer[port] = ServerPool.get_instance().get_server_transfer(port)
						ServerPool.get_instance().cb_del_server(port)
						new_servers[port] = (passwd, cfg)
						break
			else:
				new_servers[port] = (passwd, cfg)
		#关闭已失效的端口服务
		for port in cur_running:
			if port not in cur_servers:
				self.force_update_transfer[port] = ServerPool.get_instance().get_server_transfer(port)
				ServerPool.get_instance().cb_del_server(port)
		if len(new_servers) > 0:
			self.event.wait(eventloop.TIMEOUT_PRECISION + eventloop.TIMEOUT_PRECISION / 2)
			for port in new_servers.keys():
				passwd, cfg = new_servers[port]
				self.new_server(port, passwd, cfg)

	def new_server(self, port, passwd, cfg):
		protocol = cfg.get('protocol', ServerPool.get_instance().config.get('protocol', 'origin'))
		method = cfg.get('method', ServerPool.get_instance().config.get('method', 'None'))
		obfs = cfg.get('obfs', ServerPool.get_instance().config.get('obfs', 'plain'))
		logging.info('db start server at port [%s] pass [%s] protocol [%s] method [%s] obfs [%s]' % (port, passwd, protocol, method, obfs))
		for hersh in cfg:
			logging.info("%s:%s"%(hersh,cfg[hersh]))
		ServerPool.get_instance().new_server(port, cfg)

	def cmp(self, val1, val2):
		if type(val1) is bytes:
			val1 = common.to_str(val1)
		if type(val2) is bytes:
			val2 = common.to_str(val2)
		return val1 == val2

	@staticmethod
	def del_servers():
		for port in [v for v in ServerPool.get_instance().tcp_servers_pool.keys()]:
			if ServerPool.get_instance().server_is_run(port) > 0:
				ServerPool.get_instance().cb_del_server(port)

	@staticmethod
	def thread_db(obj):
		global db_instance
		last_rows = [] #上次读取的参数
		db_instance = obj()
		ServerPool.get_instance()
		try:
			while True:
				try:
					#保存端口流量记录
					db_instance.push_db_all_user()
					#读取所有端口参数
					rows = db_instance.pull_db_all_user()
					if rows:
						db_instance.pull_ok = True
					#①停止超流的服务,②重启配置更改的服务,③启动新增的服务
					db_instance.del_server_out_of_bound_safe(last_rows, rows)
					last_rows = rows
				except Exception as e:
					trace = traceback.format_exc()
					logging.error(trace)
				if db_instance.event.wait(10) or not ServerPool.get_instance().thread.is_alive():
					break
		except KeyboardInterrupt as e:
			pass
		db_instance.del_servers()
		ServerPool.get_instance().stop()
		db_instance = None

	@staticmethod
	def thread_db_stop():
		global db_instance
		db_instance.event.set()

class SqliteTransfer(TransferBase):
	def __init__(self):
		try:
			db=sqlite3.connect('./userdb.dat',isolation_level=None)
			db.row_factory=self.dict_factory
			self.cursor=db.cursor()
		except:
			logging.error("连接数据库文件'./userdb.dat'失败！")
			return
		super(SqliteTransfer, self).__init__()

	def dict_factory(self, cursor, row):
		d = {}
		for idx, col in enumerate(cursor.description):
			d[col[0]] = row[idx]
		return d

	def update_all_user(self, dt_transfer):
		'''保存所有流量增量'''
		WHEN_SUB1=''
		WHEN_SUB2=''
		PORTS=None
		for id in dt_transfer:
			if PORTS is not None:
				PORTS += ',%s' %id
			else:
				PORTS = '%s' %id
			WHEN_SUB1 += ' WHEN %s THEN u+%s' %(id,dt_transfer[id][0])
			WHEN_SUB2 += ' WHEN %s THEN d+%s' %(id,dt_transfer[id][1])
		if len(dt_transfer)>0:
			sql_str='UPDATE userlist SET u=CASE port'+WHEN_SUB1+' END,d=CASE port'+WHEN_SUB2+' END WHERE port IN ('+PORTS+')'
			self.cursor.execute(sql_str)
		return dt_transfer

	def pull_db_all_user(self):
		'''读取所有端口参数'''
		rows=self.cursor.execute("select * from userlist").fetchall()
		return rows
