#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import struct
import time
from shadowsocks import shell, eventloop, tcprelay, udprelay, asyncdns, common
import threading
import sys
import traceback
from socket import *

class MainThread(threading.Thread):
	def __init__(self, params):
		super(MainThread, self).__init__()
		self.params = params

	def run(self):
		ServerPool._loop(*self.params)

class ServerPool(object):

	instance = None

	def __init__(self):
		shell.check_python()
		self.config = shell.get_config(False)
		self.dns_resolver = asyncdns.DNSResolver()
		self.tcp_servers_pool = {}
		self.udp_servers_pool = {}
		self.loop = eventloop.EventLoop()
		self.thread = MainThread((self.loop, self.dns_resolver))
		self.thread.start()

	@staticmethod
	def get_instance():
		if ServerPool.instance is None:
			ServerPool.instance = ServerPool()
		return ServerPool.instance

	def stop(self):
		self.loop.stop()

	@staticmethod
	def _loop(loop, dns_resolver):
		try:
			dns_resolver.add_to_loop(loop)
			loop.run()
		except (KeyboardInterrupt, IOError, OSError) as e:
			logging.error(e)
			traceback.print_exc()
			os.exit(0)
		except Exception as e:
			logging.error(e)
			traceback.print_exc()

    #运行>0,停止=0
	def server_is_run(self, port):
		port = int(port)
		ret = 0
		if port in self.tcp_servers_pool:
			ret = 1
		return ret

    #运行:True;停止:False
	def server_run_status(self, port):
		if 'server' in self.config:
			if port not in self.tcp_servers_pool:
				return False
		return True

    #启动单个端口服务
	def new_server(self, port, user_config):
		port = int(port)
		if 'server' in self.config:
			if port in self.tcp_servers_pool:
				logging.info("server already at %s:%d" % (common.to_str(self.config['server']), port))
				return 'this port server is already running'
			else:
				a_config = self.config.copy()
				a_config.update(user_config)
				a_config['server_port'] = port
				a_config['max_connect'] = 128
				a_config['method'] = common.to_str(a_config['method'])
				try:
					logging.info("starting server at %s:%d" % (common.to_str(a_config['server']), port))
					tcp_server = tcprelay.TCPRelay(a_config, self.dns_resolver, False)
					tcp_server.add_to_loop(self.loop)
					self.tcp_servers_pool.update({port: tcp_server})
					udp_server = udprelay.UDPRelay(a_config, self.dns_resolver, False)
					udp_server.add_to_loop(self.loop)
					self.udp_servers_pool.update({port: udp_server})
				except Exception as e:
					logging.warn("IPV4 %s " % (e,))
		return True

    #停止单个端口服务
	def cb_del_server(self, port):
		port = int(port)
		if port not in self.tcp_servers_pool:
			logging.info("stopped server at %s:%d already stop" % (self.config['server'], port))
		else:
			logging.info("stopped server at %s:%d" % (self.config['server'], port))
			try:
				self.tcp_servers_pool[port].close()
				del self.tcp_servers_pool[port]
			except Exception as e:
				logging.warn(e)
			try:
				self.udp_servers_pool[port].close()
				del self.udp_servers_pool[port]
			except Exception as e:
				logging.warn(e)
		return True

	#获取单个端口流量
	def get_server_transfer(self, port):
		port = int(port)
		ret = [0, 0]
		if port in self.tcp_servers_pool:
			ret[0], ret[1] = self.tcp_servers_pool[port].get_ud()
		if port in self.udp_servers_pool:
			u, d = self.udp_servers_pool[port].get_ud()
			ret[0] += u
			ret[1] += d
		return ret

	#获取所有端口流量
	def get_servers_transfer(self):
		servers = self.tcp_servers_pool.copy()
		servers.update(self.udp_servers_pool)
		ret = {}
		for port in servers.keys():
			ret[port] = self.get_server_transfer(port)
		return ret
