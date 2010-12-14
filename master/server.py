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

	'''if (is_backup == 0):
		is_master = 1 # if we're not started in "backup-mode" then we must be master
	
	# okay, we need to check if we are starting up from a crash
	recoveryFile = open('recoverycheck', 'r+b')
	if (recoveryFile.read() == '1'): # uh oh, that means we did not close properly last time
		check_and_regain_master()
		
	# lets set recoveryfile to '1' which means we're currently active
	recoveryFile.seek(0, 0);
	recoveryFile.write('1')
	recoveryFile.close()'''
	
	user_server_tupple = socket.gethostname(), 5630
	get_user_server  = ThreadedTCPServer(user_server_tupple, GetPersonToCrawlHandler)
	print "Get user server created at port 5630"

	recv_data_server_tupple = socket.gethostname(), 5631
	recv_data_server = ThreadedTCPServer(recv_data_server_tupple, ReceiveDataHandler)
	print "Receive server created at port 5631"

	get_user_thread = threading.Thread(target = get_user_server.serve_forever)
	recv_data_thread = threading.Thread(target = recv_data_server.serve_forever)
	get_user_thread.daemon = True
	recv_data_thread.daemon = True
	get_user_thread.start()
	recv_data_thread.start()
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

	while should_continue:

		print 'Entered parse_data loop'
		print 'responses ' + str(responses.qsize())

		user_data = None
		try: user_data = responses.get(block=True, timeout=5)
		except Queue.Empty: pass

		if user_data:
			user_id = user_data['user']
			
			if 'followers' in user_data:
				follower_data = user_data['followers']
				for user in follower_data :
					database.insert_user(user['id'], user['name'],
						user['location'], user['description'], 0)
					database.insert_follower(user_id, user['id'])
					try: database.insert_tweet(user['id'],
							user['status']['created_at'], user['status']['text'])
					except KeyError: pass

			if 'followees' in user_data:
				followee_data = user_data['followees']
				for user in followee_data:
					database.insert_user(user['id'], user['name'],
						user['location'], user['description'], 0)
					database.insert_follower(user['id'], user_id)
					try: database.insert_tweet(user['id'],
							user['status']['created_at'], user['status']['text'])
					except KeyError: pass

			database.set_crawled(user_id)

			if to_crawl.qsize() == 0:
				print 'about to commit'
				database.save()
				uncrawled_users = database.get_unfollowed_users()
				for u in uncrawled_users:
					print 'putting %s to to_crawl' % (u[0],)
					to_crawl.put(u[0])
			print 'Data comitted to DB'

	database.save()
	database.close()


class ReceiveDataHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		global response_rate
		global response_count
		response_count = response_count + 1
		buf = self.request.recv(1024)
		data = ''
		while buf:
			data = data + buf
			buf = self.request.recv(1024)
		response = json.loads(data)
		if response: responses.put(response)
		self.request.close()
		response_rate = response_rate + 1
		response_count = response_count - 1

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
	pass

	
def find_alive_nodes(lastnode):
	backupNodesFile=open('replicate_list', 'r')
	backupNodes = backupNodesFile.read()
	backupNodesFile.close()
	nodelist = backupNodes.splitlines()

	from socket import gethostname;	
	me = gethostname()
	
	alivenode = 0
	for node in nodelist:
		if (node == lastnode):
			break
		elif (node == me):
			continue # need to skip ourselves
		elif ( os.system('ping -q -c1 ' + node) == 0):
			# okay we found a node that is alive
			# TODO: need to add method in server.py to check if node is serving requests
			alivenode = node # this means a higher priority node is master, lets tell worker thread
			break
			
	return alivenode

	
def handle_master_request():
	# we enter this function if we receive a request from a work to become the new master	
	# this fetches the hostname of this node
	from socket import gethostname;	
	myhostname = gethostname()
	
	highernodealive = find_alive_nodes(myhostname)
	
	if (highernodealive != 0 ):
		return highernodealive # alright lets just return name of higher node that is alive and serving requests
	
	# if we got this far, then it looks like we gotta become the master!
	
	# TODO: start up all server-related stuff
	is_master = 1
	return 1
	
def stop_current_master(currentMaster):
	# TODO: tell current master to stop requests while we transfer over database
	print 'TODO: tell currentmaster to stop accepting new requests'

def announce_new_master(currentMaster):
	# TODO: tell current master taht we are the new master
	print 'TODO: tell currentmaster i am new master'
	
def get_fresh_database(currentMaster):
	# we need to SCP awesomeDB from currentMaster to me
	# problem: we cannot initiate an SCP from planetlab -> reala since we don't have a public/private key pairing
	# solution: lets just get the backup from anyone
	if (currentMaster == 'reala.ece.ubc.ca'):
		currentMaster = find_alive_nodes(' ')

	cmd = "scp -i group2@eece411 usf_ubc_gnutella1@"+currentMaster+":~/birdcoop/master/awesomeDB awesomeDB"
	os.system(cmd);
		
	
def regain_master_status():
	# okay we should only be in this method for one of two reasons:
	# 1. We are REALA, and we just recovered from a failure
	# 2. We are a BACKUP node, and just recovered from a failure, AND no higher priority nodes are alive
	currentMaster = find_alive_nodes(' ')
	
	stop_current_master(currentMaster)
	get_fresh_database(currentMaster)
	
	# TODO: start up all all master server threads
	is_master = 1
	
	announce_new_master(currentMaster)


def check_and_regain_master():
	# we just recovered from failure
	# this method is a check to see if we SHOULD be master.
	# we should be master if all parent/higher priority nodes are DEAD, or if we're REALA
	from socket import gethostname;	
	myhostname = gethostname()
	
	if(is_backup == 0):
		regain_master_status()
	else:
		alive_parent_node = find_alive_nodes(myhostname)
		if (alive_parent_node == 0):
			# no parent nodes alive - we need to be master
			regain_master_status()
		else:
			# there is a parent node that's master. lets just get a database
			get_fresh_database(find_alive_nodes(' ')) 


if __name__ == '__main__': 
	sys.exit(main(*sys.argv))
	
