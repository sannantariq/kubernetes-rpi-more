#!/usr/bin/python

import socket

def test_function():
	try:
		val = socket.getfqdn();
		msg = "My name is %s";
		print msg % (val);
		return 0;
	except:
		return 1;

test_function();
