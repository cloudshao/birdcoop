import guppy
import os
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

u_id = ""

# initialize the connection, and start up our crawl list, also make the lock for renewing the crawl list
database = sql.AwesomeDatabase()
database.create_tables()
database.save()
crawl_list = database.select_unfollowed_users()
database.close()
crawl_count = 0


normal_start = 0
lock = threading.RLock()
responses = []

# Variables to keep track of some stats
clients = None
rate_history = []
connection_rate, response_rate = 0, 0
announce_count = 0
response_count = 0
is_backup = 0 # NOTE: backup and master are NOT mututally exclusive.
is_master = 0 # 	a backup is anyone EXCEPT reala.ece.ubc, and a backup can be a master if reala is down

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
		global crawl_count
		global announce_count
		announce_count = announce_count + 1
		if normal_start == 0:
			self.request.send(u_id)
			normal_start = 1
			return
			
		#print '**Worker Connection Received**'
		global lock
		global crawl_list
		global connection_rate

		#lock because we only want to get the list once - otherwise we might overwrite it
		lock.acquire()
		try:
		
			# add the client from the request to our dictionary of cliends
			clients[self.request.getpeername()[0]] = 1;
		
			if len(crawl_list) == 0 or crawl_count > 50:
				#put data in database
				#populate our crawl list
				self.database = sql.AwesomeDatabase()
				self.parse_data()
				if len(crawl_list) == 0:
					crawl_list = self.database.select_unfollowed_users()
				self.database.close()
				
		finally:
			lock.release()
	
		if crawl_list:
			user = crawl_list.pop()[0]
		else:
			user = 0
		self.request.send(str(user))
		self.request.close()
		connection_rate = connection_rate + 1
		announce_count = announce_count - 1

	#Parses the data for a series of users and puts it in the database
	def parse_data(self) :
		global crawl_count
		print "About to parse data and populate the database"
		# Each machine can only crawl 150 followers per hour, so track the number of users crawled
		# Depending on the rate responses come in, we may need to lock so that we don't loop infinitely
		while len(responses) > 0:
			user_data = responses.pop()
			user_id = user_data['user']
			
			# Response doesn't have followers if user was private
			if 'followers' in user_data:
				follower_data = user_data['followers']
				for user in follower_data :
					pass
					# insert the users data into our user_table
					self.database.insert_user(user['id'], user['name'], user['location'], user['description'], 0)
					# this user follows user_name - insert his/her in followers
					self.database.insert_follower(user_id, user['id'])
					try: 
						self.database.insert_tweet(user['id'], user['status']['created_at'], user['status']['text']) #Try inserting this users status - if he has one it will insert
					except KeyError:
						# User does not have a status
						pass

			# Response doesn't have followees if user was private
			if 'followees' in user_data:
				followee_data = user_data['followees']
				for user in followee_data:
					#insert the users data into our user_table
					self.database.insert_user(user['id'], user['name'], user['location'], user['description'], 0)
					# this user follows user_name - insert him/her in follower_table
					self.database.insert_follower(user['id'], user_id)
					try: 
						self.database.insert_tweet(user['id'], user['status']['created_at'], user['status']['text']) #Try inserting this users status - if he has one it will insert
					except KeyError:
						# User does not have a status
						pass

			# This user with username user_name has now been crawled
			self.database.set_crawled(user_id)

		# write everything we have added to disk
		crawl_count = 0
		print "about to commit"
		self.database.save()
		print "Data comitted to DB"


class ReceiveDataHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		global crawl_count
		global response_rate
		global response_count
		response_count = response_count + 1
		crawl_count = crawl_count+1
		#print "Crawl results connection received"
		buf = self.request.recv(1024)
		data = ''
		while buf:
			data = data + buf
			buf = self.request.recv(1024)
		#print "loading json"
		response = json.loads(data)
		if response:
			responses.append(response)
		#print "Followers received on server"
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
	
	gc.set_debug(gc.DEBUG_LEAK)
	for arg in sys.argv: 
		name = arg
		if name == "BACKUP" :
			is_backup = 1
		elif name != "server.py" :  #Is the arg actaully a username, or this file's name? - Can't find way to get filename from inside code - hopefully we don't rename
			u_id = name

			
	if (is_backup == 0):
		is_master = 1 # if we're not started in "backup-mode" then we must be master
	
	# okay, we need to check if we are starting up from a crash
	recoveryFile = open('recoverycheck', 'r+b')
	if (recoveryFile.read() == '1'): # uh oh, that means we did not close properly last time
		check_and_regain_master()
		
	# lets set recoveryfile to '1' which means we're currently active
	recoveryFile.seek(0, 0);
	recoveryFile.write('1')
	recoveryFile.close()
	
	normal_start = 1
	if len(crawl_list) == 0:
		normal_start = 0;
	clients = {}

	start_id = u_id

	user_server_tupple = socket.gethostname(), 5630
	get_user_server  = ThreadedTCPServer(user_server_tupple, GetPersonToCrawlHandler)

	print "Get user server created at port 5630"
	recv_data_server_tupple = socket.gethostname(), 5631
	recv_data_server = ThreadedTCPServer(recv_data_server_tupple, ReceiveDataHandler)

	print "Receive server created at port 5631"
	get_user_thread = threading.Thread(target = get_user_server.serve_forever)
	recv_data_thread = threading.Thread(target = recv_data_server.serve_forever)

	print "Server threads created"
	get_user_thread.daemon = True
	recv_data_thread.daemon = True
	get_user_thread.start()
	recv_data_thread.start()
	print "Server's started and waiting for input"

	# Start a thread to keep track of hourly crawl rate
	rate_track_thread = threading.Thread(target=rate_tracker_thread)
	rate_track_thread.daemon = True
	rate_track_thread.start()

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
			print 'responses: ' + str(len(responses))
			print 'announce_handlers: ' + str(announce_count)
			print 'response_handlers: ' + str(response_count)
		elif 'heap' in line:
			print guppy.hpy().heap()
		elif 'exit' in line:
			recoveryFile = open('recoverycheck', 'w')
			recoveryFile.write('0') #need to set recovery file to 0, which means we closed properly
			recoveryFile.close()
			sys.exit()
		elif 'garbage' in line:
			x = gc.collect()
			print x
		elif not line.strip():
			print 'type "exit" to terminate'

		else:
			print 'Did not understand your command'

