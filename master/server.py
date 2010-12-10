import guppy
import socket
import SocketServer
import string
import threading
import time
import sqlite3
import sys
import simplejson as json

def init(cursor):

	#check to see if user table was created
	cursor.execute("select name from sqlite_master Where type='table' and Name= 'user_table'")

	if cursor.fetchone() == None :
		# user table has not yet been created, create it
		print"Creating user table"
		cursor.execute("create table user_table (user_id integer PRIMARY KEY, name char(20), location char(30), bio char(160), currTime INTEGER)")


	#check to see if user crawled table was created
	cursor.execute("select name from sqlite_master Where type='table' and Name= 'user_crawled_table'")

	if cursor.fetchone() == None :
		# user crawled table has not yet been created, create it
		print"Creating user crawled table"
		# this table tracks if a user has been crawled or not
		cursor.execute("create table user_crawled_table (user_id integer PRIMARY KEY, crawled integer, currTime  integer)")

	# check to see if followers table has been created
	cursor.execute("select name from sqlite_master Where type='table' and Name= 'follower_table'")

	if cursor.fetchone() == None :
		# followers table has not yet been created, create it
		print"Creating follower table"
		cursor.execute("create table follower_table (user_id integer, follower_id integer, currTime INTEGER, PRIMARY KEY(user_id, follower_id))")

	# check to see if tweet table has been created
	cursor.execute("select name from sqlite_master Where TYPE='table' and NAME = 'tweet_table'")

	if cursor.fetchone() == None :
		print"Creating tweet table"
		# tweet table has not yet been created, create it
		cursor.execute("create table tweet_table (user_id integer, time CHAR(20), tweet CHAR(140), currTime INTEGER, PRIMARY KEY(user_id, time))")

def drop_tables(cursor) :
	
	#check to see if user table was created
	cursor.execute("select name from sqlite_master Where type='table' and Name= 'user_table'")

	if cursor.fetchone() != None :
		# user table has been created, drop it
		print"Dropping user_table"
		cursor.execute("Drop table 'user_table'")

	#check to see if user_crawled_table was created
	cursor.execute("select name from sqlite_master Where type='table' and Name= 'user_crawled_table'")

	if cursor.fetchone() != None :
		# user crawled table has been created, drop it
		print"Dropping user_crawled_table"
		cursor.execute("Drop table 'user_crawled_table'")
	
	# check to see if followers table has been created
	cursor.execute("select name from sqlite_master Where type='table' and Name= 'follower_table'")

	if cursor.fetchone() != None :
		# followers table has been created, drop it
		print"Dropping followers_table"
		cursor.execute("Drop table 'follower_table'")

	
	# check to see if tweet_table has been created
	cursor.execute("select name from sqlite_master Where TYPE='table' and NAME = 'tweet_table'")

	if cursor.fetchone() != None :
		# tweet table has been created, drop it
		print"Dropping tweet table"
		cursor.execute("Drop table 'tweet_table'")

# finds a users which are not crawlwed and returns them
def select_unfollowed_users(cursor):
	cursor.execute('Select user_id from user_crawled_table where crawled == 0')
	user = cursor.fetchall()
	return user;

# inserts one user into user table
def insert_user(cursor, user_id, name, location, bio) :
	try:
		#print"Try inserting " + str(user_id)
		t = (user_id, name, location, bio, int(time.time()))
		cursor.execute('Insert into user_table(user_id, name, location, bio, currTime) values(?,?,?,?,?)', t)
	except sqlite3.IntegrityError:
		# the user was already in the table - so just continue
		#print"Duplicate user"

# inserts one user into user_crawled_table - used for for finding users which haven't been craweld
def insert_user_crawled(cursor, user_id, wasCrawled) :
	try:
		#print"Try inserting if " + str(user_id) + " was crawled"
		t = (user_id, wasCrawled, int(time.time()))
		cursor.execute('Insert into user_crawled_table(user_id, crawled, currTime) values(?,?,?)', t)
	except sqlite3.IntegrityError :
		if wasCrawled  == 1:
			# User is already in DB, update wasCrawled if it is now true - (ie this is the user we are crawling) - not in all cases because we don't want to set a 
			# crawled user back to false
			#print"User already in db, but it's now crawled so setting it to crawled"
			cursor.execute('Insert or Replace into user_crawled_table(user_id, crawled, currTime) values(?,?,?)', t)


# inserts a user- follower relation into the follower_table
def insert_follower(cursor, user_id, follower_id) :
	try:
		t = (user_id, follower_id, int(time.time()))
		cursor.execute('Insert into follower_table(user_id, follower_id, currTime) values(?, ?,?)', t)
	except sqlite3.IntegrityError :
		#print "Tried to insert follower relation we had already inserted"

# inserts a user-tweet relation into the db
def insert_tweet(cursor, user_id, tweet_time, tweet) :
	try:
		t = (user_id, tweet_time, tweet, time.time())
		cursor.execute('Insert into tweet_table(user_id, time, tweet, currTime) values (?, ?, ?, ?)', t)
		#print "tweet inserted for" + str(user_id)
	except sqlite3.IntegrityError:
		#print "tried to insert tweet we had already inserted"


u_id = ""

# initialize the connection, and start up our crawl list, also make the lock for renewing the crawl list
conn = sqlite3.connect("awesomeDB2")
cursor = conn.cursor() 
#drop_tables(cursor)
init(cursor)
crawl_list  = select_unfollowed_users(cursor)
crawl_count = 0
cursor.close()

conn.close()


normal_start = 0
lock = threading.RLock()
responses = []

# Variables to keep track of some stats
clients = None
rate_history = []
connection_rate, response_rate = 0, 0

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
		if normal_start == 0:
			self.request.send(u_id)
			normal_start = 1
			return
			
		print '**Worker Connection Received**'
		global lock
		global crawl_list
		global connection_rate

		#lock because we only want to get the list once - otherwise we might overwrite it
		lock.acquire()
		try:
		
			# add the client from the request to our dictionary of cliends
			clients[self.request.getpeername()[0]] = 1;
		
			if len(crawl_list) == 0 or crawl_count > 500:
				#put data in database
				#populate our crawl list
				self.conn = sqlite3.connect("awesomeDB2")
				self.cursor = self.conn.cursor()
				self.parse_data()
				if len(crawl_list) == 0:
					crawl_list = select_unfollowed_users(self.cursor);
				self.cursor.close()
				self.conn.close()
				
		finally:
			lock.release()
	
		if crawl_list:
			user = crawl_list.pop()[0]
		else:
			user = 0
		self.request.send(str(user))
		self.request.close()
		connection_rate = connection_rate + 1

	#Parses the data for a series of users and puts it in the database
	def parse_data(self) :
		global crawl_count
		print "About to parse data and populate the database"
		# Each machine can only crawl 150 followers per hour, so track the number of users crawled
		# Depending on the rate responses come in, we may need to lock so that we don't loop infinitely
		while len(responses) > 0 :
			user_data = responses.pop()
			user_id = user_data['user']
			
			# Response doesn't have followers if user was private
			if 'followers' in user_data:
				follower_data = user_data['followers']
				for user in follower_data :
					# insert the users data into our user_table
					insert_user(self.cursor, user['id'], user['name'], user['location'], user['description'])
					# AFAIK this user has not been crawled yet - if so insert_user_crawled will handle it
					insert_user_crawled(self.cursor, user['id'], 0)
					# this user follows user_name - insert his/her in followers
					insert_follower(self.cursor, user_id, user['id'])
					try: 
						insert_tweet(self.cursor, user['id'], user['status']['created_at'], user['status']['text']) #Try inserting this users status - if he has one it will insert
					except KeyError:
						# User does not have a status
						pass

			# Response doesn't have followees if user was private
			if 'followees' in user_data:
				followee_data = user_data['followees']
				for user in followee_data:
					# insert the users data into our user_table
					insert_user(self.cursor, user['id'], user['name'], user['location'], user['description'])
					# AFAIK this user has not been crawled yet - if so insert_user_crawled will handle it
					insert_user_crawled(self.cursor, user['id'], 0)
					# this user follows user_name - insert him/her in follower_table
					insert_follower(self.cursor, user['id'], user_id)
					try: 
						insert_tweet(self.cursor, user['id'], user['status']['created_at'], user['status']['text']) #Try inserting this users status - if he has one it will insert
					except KeyError:
						# User does not have a status
						pass

			# This user with username user_name has now been crawled
			insert_user_crawled(self.cursor, user_id, 1)

		# write everything we have added to disk
		crawl_count = 0
		self.conn.commit()
		print "Data comitted to DB"


class ReceiveDataHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		global crawl_count
		global response_rate
		crawl_count = crawl_count+1
		#print "Crawl results connection received"
		buf = self.request.recv(1024)
		data = ''
		while buf:
			data = data + buf
			buf = self.request.recv(1024)
		#print "loading json"
		response  = json.loads(data)
		responses.append(response)
		#print "Followers received on server"
		self.request.close()
		response_rate = response_rate + 1
	
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
	pass



if __name__ == '__main__':	

	for arg in sys.argv: 
		name = arg
		if name != "server.py" :  #Is the arg actaully a username, or this file's name? - Can't find way to get filename from inside code - hopefully we don't rename
			u_id = name

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
	get_user_thread.setDaemon(True)
	recv_data_thread.setDaemon(True)
	get_user_thread.start()
	recv_data_thread.start()
	print "Server's started and waiting for input"

	# Start a thread to keep track of hourly crawl rate
	threading.Thread(target=rate_tracker_thread).start()

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
		elif 'heap' in line:
			print guppy.hpy().heap()
		elif 'exit' in line:
			conn.close()
			sys.exit()
		elif not line.strip():
			print 'type "exit" to terminate'
		else:
			print 'Did not understand your command'

