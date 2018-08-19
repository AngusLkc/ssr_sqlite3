#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import sys
import threading
import os

if __name__ == '__main__':
	import inspect
	os.chdir(os.path.dirname(os.path.realpath(inspect.getfile(inspect.currentframe()))))

import server_pool
import db_transfer
from shadowsocks import shell

class MainThread(threading.Thread):
	def __init__(self, obj):
		super(MainThread, self).__init__()
		self.daemon = True
		self.obj = obj

	def run(self):
		self.obj.thread_db(self.obj)

	def stop(self):
		self.obj.thread_db_stop()

def main():
	shell.check_python()
	thread = MainThread(db_transfer.SqliteTransfer)
	thread.start()
	try:
		while thread.is_alive():
			thread.join(10.0)
	except (KeyboardInterrupt, IOError, OSError) as e:
		import traceback
		traceback.print_exc()
		thread.stop()

if __name__ == '__main__':
	main()
