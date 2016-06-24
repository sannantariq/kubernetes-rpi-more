#!/usr/bin/env python

import socket;
import select;
import pickle;
import json;
import time;
import sys;


def divide(input, num):
	"""Manuel Zubeita's answer from stack overflow
		can be found at:
		http://stackoverflow.com/questions/2659900/python-slicing-a-list-into-n-nearly-equal-length-partitions"""
	return [ input[i::num] for i in xrange(num) ]


class MapObj(object):
	"""An object consisting of a data set and a callable function
	"""
	def __init__(self, dataset):
		self.dataset = dataset;

	def fun_run(self):
		pass;

	def full_reduce(self, data):
		pass;
		
class CountPrimes(MapObj):
	"""Function that counts the primes in a data set"""
	def __init__(self, dataset):
		super(CountPrimes, self).__init__(dataset);
		self.base_case = 0;
	
	def map_fun(self, n):
		if n < 2:
			return 0;
		i = 2
		while i * i <= n:
			if n % i == 0:
				return 0;
			i += 1;
		return 1;

	def red_fun(self, n, m):
		return n + m;

	def fun_run(self):
		return reduce(self.red_fun, map(self.map_fun, self.dataset), self.base_case);

	@staticmethod
	def full_reduce(data):
		return reduce(lambda x, y: x + y, data, 0);

class Master(object):
	"""This is the server"""
	def __init__(self, host = 'localhost', port = 50000, log_run = True, backlog = 5, buffer_size = 1024, dataset = range(100), min_thr = 2, obj = CountPrimes):
		self.log_run = log_run;
		self.data = dataset;
		self.client_list = [];
		self.buffer_size = buffer_size;
		self.obj = obj;


		server = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
		server.bind((host,port)); 
		server.listen(backlog);
		fds = [server];
		running = 1;
		map_run = False;
		red_run = False;
		reduce_data = [];

		while running:
			input_ready, o, e = select.select(fds, [], [], 1);
			self.log('Checking for other things, map run = %r, red run = %r' % (map_run, red_run));

			for fd in input_ready:
				self.log('Ready input...');
				if fd == server:
					client, addr = server.accept();
					self.log('Got connection from %s' % json.dumps(addr));
					self.client_list.append(client);
					fds.append(client);

				else:
					self.log('Receiving data.')
					data = fd.recv(self.buffer_size);
					if data:
						reduce_data.append(json.loads(data));
						self.log('received response: %s' % data);
					else:
						self.log('client left');
						fd.close();
						fds.remove(fd);
						self.client_list.remove(fd);

			if not map_run:
				if len(self.client_list) >= min_thr:
					self.log('Running map');
					res_count = self.map_data(dataset);
					map_run = True;
					self.log('Should get %d reponses' % res_count)

			if map_run and not red_run:
				if len(reduce_data) == res_count:
					self.log('Running reduce on %s' % (json.dumps(reduce_data)));
					answer = CountPrimes.full_reduce(reduce_data);
					running = 0;
					red_run = True;

		server.close();
		print 'The answer was %d.' % (answer);

	def log(self, msg):
		if self.log_run:
			print '<SERVER SAYS> %s' % msg;

	def map_data(self, dataset):
		partitions = divide(self.data, len(self.client_list));
		data_to_send = zip(partitions, self.client_list);
		print "Data to send >>> " ,data_to_send
		for (d, c) in data_to_send:
			c.send(pickle.dumps(self.obj(d)) + '\n');
		return len(partitions);

class Worker(object):
	"""This is a woker"""
	def __init__(self, serv_addr = 'localhost', serv_port = 50000, log_run = True, buffer_size = 1024):
		self.log_run = log_run;
		conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
		try:
			conn.connect((serv_addr, serv_port));
			self.log('Connected to server, receiving task.');
			data = conn.recv(buffer_size);
			self.log('task received, parsing, executing and sending.')
			obj = pickle.loads(data);
			res = json.dumps(obj.fun_run());
			self.log('local response: %s for data: %s' % (res, json.dumps(obj.dataset)));
			print conn.send(res +  '\n');
			self.log('task done and sent.')

		except socket.error:
			self.log('Error connecting');
		except socket.timeout:
			self.log('Connection timeout');
		finally:
			conn.close();
			time.sleep(1);
			print "Good bye"

	def log(self, msg):
		if self.log_run:
			print '<CLIENT SAYS> %s' % msg;

def usage():
	usage_string = "Usage: %s [-m | -w] [host] [port]"
	print usage_string;
	sys.exit(0);


def main():
	if len(sys.argv) < 2:
		usage();

	run_type = sys.argv[1];
	if run_type == '-m':
		Master();
	elif run_type == '-w':
		Worker();
	else:
		usage();


if __name__ == '__main__':
	main()
		



		
