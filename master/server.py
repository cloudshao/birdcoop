import socket
import SocketServer
import threading
import sqlite3
import request
import sys
import json
import urllib2

u_id = ""

craw_list = None
normal_start = None
lock = threading.RLock()
clients = None
# initialize the connection, and start up our crawl list, also make the lock for renewing the crawl list
conn = sqlite3.connect("awesomeDB")
cursor = conn.cursor() 
pending_list = None
responses = None

class GetPersonToCrawlHandler(SocketServer.BaseRequestHandler):

	def handle(self):
		if normal_start == 0:
			self.request.send(u_id)
			return
			
		print '**Worker Connection Received**'
		try:
			#lock because we only want to get the list once - otherwise we might overwrite it
			lock.aquire()
		
			# add the client from the request to our dictionary of cliends
			clients[self.request.getpeername()[0]] = 1;
		
			while len(crawl_list) == 0 :
				#put data in database
				#populate our crawl list
				crawl_list = select_unfollowed_user(cursor);
				self.parse_data()
		finally:
			lock.release()
		
		user = crawl_list.pop()[0]
		self.request.send(str(user))
		pending_list.extend(user)

		#Parses the data for a series of users and puts it in the database

	def parse_data() :
		# Each machine can only crawl 150 followers per hour, so track the number of users crawled
		# Depending on the rate responses come in, we may need to lock so that we don't loop infinitely
		while len(responses) > 0 :
			user_data = json.loads(response.pop())
			user_id = user_data['user']	
			follower_data = user_data['followers']
			print "Beginning parsing data for user " + str(user_id) + " Num crawled: " + str(num_crawled)
			# Get the the json user data from twitter, and load it into something we can use
			for user in follower_data :
				print 'This is a follower with name ' + user['name']
				insert_user(cursor, user['id'], user['name'], user['location'], user['description']) # insert the users data into our user_table
				insert_user_crawled(cursor, user['id'], 0) # AFAIK this user has not been crawled yet - if so insert_user_crawled will handle it
				insert_follower(cursor, user_id, user['id']) # this user follows user_name - insert him/her in follower_table
				try: 
					print "inserting tweet for " + str(user['id'])
					insert_tweet(cursor, user['id'], user['status']['created_at'], user['status']['text']) #Try inserting this users status - if he has one it will insert
				except KeyError:
					# User does not have a status - so we caught a keyError
					print "User has no tweets"
			followee_data = user_data['followees']
			for user in followee_data:
				print 'This is a followee with name ' + user['name']
				insert_user(cursor, user['id'], user['name'], user['location'], user['description']) # insert the users data into our user_table
				insert_user_crawled(cursor, user['id'], 0) # AFAIK this user has not been crawled yet - if so insert_user_crawled will handle it
				insert_follower(cursor, user['id'], user_id) # this user follows user_name - insert him/her in follower_table
				try: 
					print "inserting tweet for " + str(user['id'])
					insert_tweet(cursor, user['id'], user['status']['created_at'], user['status']['text']) #Try inserting this users status - if he has one it will insert
				except KeyError:
					# User does not have a status - so we caught a keyError
					print "User has no tweets"
			# This user with username user_name has now been crawled
			insert_user_crawled(cursor, user_id, 1)
		# write everything we have added to disk
		cursor.commit()


class ReceiveDataHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		buf = self.request.recv(1024)
		data = ''
		while buf:
			data = data + buf
			buf = self.request.recv(1024)
		responses.extend(data)
			
	

# finds a users which are not crawlwed and returns them
def select_unfollowed_users(cursor):
	cursor.execute('Select user_id from user_crawled_table where crawled == 0')
	user = cursor.fetchall()
	return user;

# inserts one user into user table
def insert_user(cursor, user_id, name, location, bio) :
	try:
		print"Try inserting " + str(user_id)
		t = (user_id, name, location, bio)
		cursor.execute('Insert into user_table(user_id, name, location, bio) values(?,?,?,?)', t)
	except sqlite3.IntegrityError:
		# the user was already in the table - so just continue
		print"Duplicate user"

# inserts one user into user_crawled_table - used for for finding users which haven't been craweld
def insert_user_crawled(cursor, user_id, wasCrawled) :
	try:
		print"Try inserting if " + str(user_id) + " was crawled"
		t = (user_id, wasCrawled)
		cursor.execute('Insert into user_crawled_table(user_id, crawled) values(?,?)', t)
	except sqlite3.IntegrityError :
		if wasCrawled  == 1:
			# User is already in DB, update wasCrawled if it is now true - (ie this is the user we are crawling) - not in all cases because we don't want to set a 
			# crawled user back to false
			print"User already in db, but it's now crawled so setting it to crawled"
			cursor.execute('Insert or Replace into user_crawled_table(user_id, crawled) values(?,?)', t)

# inserts a user- follower relation into the follower_table
def insert_follower(cursor, user_id, follower_id) :
	try:
		t = (user_id, follower_id)
		cursor.execute('Insert into follower_table(user_id, follower_id) values(?, ?)', t)
	except sqlite3.IntegrityError :
		print "Tried to insert follower relation we had already inserted"

# inserts a user-tweet relation into the db
def insert_tweet(cursor, user_id, time, tweet) :
	try:
		t = (user_id, time, tweet)
		cursor.execute('Insert into tweet_table(user_id, time, tweet) values (?, ?, ?)', t)
		print "tweet inserted for" + str(user_id)
	except sqlite3.IntegrityError:
		print "tried to insert tweet we had already inserted"

def init(cursor):

	#check to see if user table was created
	cursor.execute("select name from sqlite_master Where type='table' and Name= 'user_table'")

	if cursor.fetchone() == None :
		# user table has not yet been created, create it
		print"Creating user table"
		cursor.execute("create table user_table (user_id integer PRIMARY KEY, name char(20), location char(30), bio char(160))")


	#check to see if user crawled table was created
	cursor.execute("select name from sqlite_master Where type='table' and Name= 'user_crawled_table'")

	if cursor.fetchone() == None :
		# user crawled table has not yet been created, create it
		print"Creating user crawled table"
		# this table tracks if a user has been crawled or not
		cursor.execute("create table user_crawled_table (user_id integer PRIMARY KEY, crawled integer)")

	# check to see if followers table has been created
	cursor.execute("select name from sqlite_master Where type='table' and Name= 'follower_table'")

	if cursor.fetchone() == None :
		# followers table has not yet been created, create it
		print"Creating follower table"
		cursor.execute("create table follower_table (user_id integer, follower_id integer, PRIMARY KEY(user_id, follower_id))")

	# check to see if tweet table has been created
	cursor.execute("select name from sqlite_master Where TYPE='table' and NAME = 'tweet_table'")

	if cursor.fetchone() == None :
		print"Creating tweet table"
		# tweet table has not yet been created, create it
		cursor.execute("create table tweet_table (user_id integer, time CHAR(20), tweet CHAR(140), PRIMARY KEY(user_id, time))")

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

if __name__ == '__main__':
	# initialize the connection, and start up our crawl list, also make the lock for renewing the crawl list
	conn = sqlite3.connect("awesomeDB")
	cursor = conn.cursor() 	

	for arg in sys.argv: 
		name = arg
		if name != "server.py" :  #Is the arg actaully a username, or this file's name? - Can't find way to get filename from inside code - hopefully we don't rename
			u_id = name

	craw_list = select_unfollowed_users(cursor)
	normal_start = 1
	if len(craw_list) == 0:
		normal_start = 0;
	clients = {}

	start_id = u_id

	user_server_tupple = socket.gethostname(), 5630
	get_user_server  = SocketServer.TCPServer(user_server_tupple, GetPersonToCrawlHandler)

	print "Get user server created at port 5630"
	recv_data_server_tupple = socket.gethostname(), 5631
	recv_data_server = SocketServer.TCPServer(recv_data_server_tupple, ReceiveDataHandler)

	print "Receive server created at port 5631"
	get_user_thread = threading.Thread(target = get_user_server.serve_forever)
	recv_data_thread = threading.Thread(target = recv_data_server.serve_forever)

	print "Server threads created"
	get_user_thread.setDaemon(True)
	recv_data_thread.setDaemon(True)
	get_user_thread.start()
	recv_data_thread.start()
	print "Server's started and waiting for input"
	print "Press <Enter> to exit"

	
	sys.stdin.readline()
	
	conn.close()
