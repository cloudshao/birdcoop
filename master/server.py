import guppy
import os
import Queue
import socket
import SocketServer
import string
import threading
import time
import sql
import sqlite3
import sys
import simplejson as json
import gc
import recovery

# initialize the connection, and start up our crawl list, also make the lock for renewing the crawl list

to_crawl = Queue.Queue()
responses = Queue.Queue()

normal_start = 0
lock = threading.RLock()

# Variables to keep track of some stats
clients = {}
rate_history = []
connection_rate, response_rate = 0, 0
announce_count = 0
response_count = 0
is_backup = 0 # NOTE: backup and master are NOT mututally exclusive.
is_master = 0 # 	a backup is anyone EXCEPT reala.ece.ubc, and a backup can be a master if reala is down

def main(*args):

	global should_continue
	global is_master

	try:
		is_backup = (args[2] == 'BACKUP')
	except IndexError:
		is_backup = False

	initial_d = None
	try:
		initial_d = int(args[1])
		to_crawl.put(initial_d)
	except IndexError, ValueError: pass

	print 'is_backup ' + str(is_backup)
	print 'initial_d ' + str(initial_d)
	print 'to_crawl ' + str(to_crawl.qsize())

	if not is_backup:
		print 'We are not in backup node, so lets become master.'
		is_master = True # if we're not started in "backup-mode" then we must be master
	else:
		print 'We are a backup node. We cannot accept requests for workers.'
	
	# okay, we need to check if we are starting up from a crash
	if (os.path.isfile('recoverycheck') == 0):
		newfile = open('recoverycheck', 'w')
		newfile.write('0')
		newfile.close()
		
	recoveryFile = open('recoverycheck', 'rb+')
	status = recoveryFile.read()
	if ('1' in status): # uh oh, that means we did not close properly last time
		is_master = recovery.check_and_regain_master(is_master, is_backup)
		
	# lets set recoveryfile to '1' which means we're currently active
	recoveryFile.seek(0, 0)
	recoveryFile.write('1')
	recoveryFile.close()

	
	user_server_tupple = socket.gethostname(), 5630
	get_user_server  = ThreadedTCPServer(user_server_tupple, GetPersonToCrawlHandler)
	print "Get user server created at port 5630"

	recv_data_server_tupple = socket.gethostname(), 5631
	recv_data_server = ThreadedTCPServer(recv_data_server_tupple, ReceiveDataHandler)
	print "Receive server created at port 5631"

	control_server_tupple = socket.gethostname(), 5632
	control_server = ThreadedTCPServer(control_server_tupple, ControlMessageHandler)
	print "Control server created at port 5632"
	
	
	get_user_thread = threading.Thread(target = get_user_server.serve_forever)
	recv_data_thread = threading.Thread(target = recv_data_server.serve_forever)
	control_thread = threading.Thread(target = control_server.serve_forever)
	get_user_thread.daemon = True
	recv_data_thread.daemon = True
	control_thread.daemon = True
	get_user_thread.start()
	recv_data_thread.start()
	control_thread.start()
	print "Server threads created"

	# Start a thread to keep track of hourly crawl rate
	rate_track_thread = threading.Thread(target=rate_tracker_thread)
	rate_track_thread.daemon = True
	rate_track_thread.start()

	# Start a thread for parsing
	# Not a daemon because we want it to complete its actions properly
	parser_thread = threading.Thread(target=parse_data_thread)
	parser_thread.start()

	print "Server's started and waiting for input"

	while True:
		line = sys.stdin.readline()
		if 'workers' in line:
			for k in clients:
				print k
			print 'unique workers seen: ' + str(len(clients))
		elif 'rate' in line:
			print 'rates:'
			for r in rate_history:
				print r
			print ('connections, responses this hour: ' +
					 str(connection_rate) + ', ' + str(response_rate))
		elif 'lists' in line:
			print 'crawl_list: ' + str(len(crawl_list))
			print 'responses: ' + str(responses.qsize())
			print 'announce_handlers: ' + str(announce_count)
			print 'response_handlers: ' + str(response_count)
		elif 'heap' in line:
			print guppy.hpy().heap()
		elif 'exit' in line:
			should_continue = False
			recoveryFile = open('recoverycheck', 'w')
			recoveryFile.write('0') #need to set recovery file to 0, which means we closed properly
			recoveryFile.close()
			break
		elif 'garbage' in line:
			x = gc.collect()
			print x
		elif not line.strip():
			print 'type "exit" to terminate'

		else:
			print 'Did not understand your command'

	print 'Exiting'
	return 0


def rate_tracker_thread():

	global connection_rate, response_rate
	while True:
		# Every hour
		time.sleep(60*60)

		# Save the rate from the past hour
		rate_history.append((connection_rate, response_rate))

		# Restart the counter
		connection_rate, response_rate = 0, 0

class GetPersonToCrawlHandler(SocketServer.BaseRequestHandler):

	def handle(self):
		global normal_start
		global announce_count
		global connection_rate

		announce_count = announce_count + 1
		
		if not is_master:
			print 'We have a received a request from a worker, but we are not a master node.'
			self.request.send('-1')
			
		else:
			clients[self.request.getpeername()[0]] = 1;
			
			try:
				user = to_crawl.get(block=True, timeout=5)
				print 'sending ' + str(user)
				self.request.send(str(user))
			except Queue.Empty:
				print 'sending ' + str(0)
				self.request.send(str(0))

		self.request.close()
		connection_rate = connection_rate + 1
		announce_count = announce_count - 1

def parse_data_thread():

	global should_continue
	should_continue = True

	database = sql.AwesomeDatabase()
	database.create_tables()
	database.save()

	num_uncommitted = 0

	while should_continue:

		print 'Entered parse_data loop'
		print 'responses ' + str(responses.qsize())

		user_data = None
		try: user_data = responses.get(block=True, timeout=5)
		except Queue.Empty: pass

		if user_data:
			num_uncommitted += 1
			user_id = user_data['user']
			
			if 'followers' in user_data:
				follower_data = user_data['followers']
				for user in follower_data :
					database.insert_user(user['id'], user['name'],
						user['location'], 0)
					database.insert_follower(user_id, user['id'])

			if 'followees' in user_data:
				followee_data = user_data['followees']
				for user in followee_data:
					database.insert_user(user['id'], user['name'],
						user['location'], 0)
					database.insert_follower(user['id'], user_id)

			database.set_crawled(user_id)

			if to_crawl.qsize() == 0 or num_uncommitted > 500:

				print 'about to commit'
				database.save()
				print 'Data comitted to DB'

				num_uncommitted = 0

				if to_crawl.qsize() == 0:
					uncrawled_users = database.get_unfollowed_users()
					for u in uncrawled_users:
						print 'putting %s to to_crawl' % (u[0],)
						to_crawl.put(u[0])

	database.save()
	database.close()


class ReceiveDataHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		global response_rate
		global response_count
		
		if is_master:
			response_count = response_count + 1
			buf = self.request.recv(1024)
			data = ''
			while buf:
				data = data + buf
				buf = str(self.request.recv(1024))
			response = json.loads(data)
			if response: responses.put(response)
			self.request.close()
		response_rate = response_rate + 1
		response_count = response_count - 1

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
	pass

	
class ControlMessageHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		global is_master
		
		msg = self.request.recv(1024)
		print 'Received a control message: '+msg
		if 'become_master' in msg:
			status,is_master = recovery.handle_master_request(is_master)
			self.request.send(str(status))
		elif 'is_master' in msg:
			self.request.send(str(is_master));
		elif 'stop_master' in msg:
			is_master = False;


if __name__ == '__main__': 
	sys.exit(main(*sys.argv))
	
