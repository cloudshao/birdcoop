import request
import simplejson as json
import socket
import sys
import urllib2
import os

ANNOUNCE_PORT = 5630
REPLY_PORT = 5631
CONTROL_PORT = 5632


def main(*args):
	"""
	Asks the master for a node to crawl, and sends back the crawl results.

	Positonal argument 1: the hostname of the master
	"""

	# Get the hostname positional argument
	try:
		host = args[1]
		oldHost = ''
	except IndexError:
		print 'Usage: python test_worker.py <master>'
		return 1

	# Keep processing until rate limit is reached
	user_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	reply_socket =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	
	try:
		user_socket.connect((host, ANNOUNCE_PORT))
		reply_socket.connect((host, REPLY_PORT))
	except socket.error:
		print "Server not running, Cannot proceed"
		return 1
		
	rate_limit_reached = False
	while not rate_limit_reached:
		
		print 'Current host: '+str(host)
		try:		
			# Get the user to crawl from the master
			print 'Getting user to crawl...'
			user_socket.send("USER")
			user = int(user_socket.recv(1024))
			#user = announce(host, ANNOUNCE_PORT)	
			print 'Got user '+str(user)
		except:
			user = -1
			print 'Received exception. Looking for another host.'
			host = find_alive_master_nodes()
			
			if (oldHost != host):
				user_socket.close()
				reply_socket.close()
				user_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				reply_socket =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				user_socket.connect((host, ANNOUNCE_PORT))
				reply_socket.connect((host, REPLY_PORT))
				
			while 1==1:
				if (host == 0):
					print 'Could not find any replicas that are master nodes. Lets request highest priority replica to become master.'
					host = find_alive_nodes()
					if (host == 0):
						print 'No replica nodes are alive. Lets quit.'
						sys.exit()
					sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					sock.connect((host,CONTROL_PORT))
					print 'Requesting node '+str(host)+' to become new master'
					sock.send('become_master')
					status = sock.recv(1024)
					sock.close()
					if ('1' not in status):
						print 'It seems there is already another master up.'
						host = find_alive_master_nodes()
					else:
						print 'Great, host '+str(host)+' is the new master node!'
						user = announce(host, ANNOUNCE_PORT)
						
				else:
					break
				
			
		# did we get a -1 response? if so we need to look for the new master
		if (user == -1):
			print 'Whoops, it looks like we contacted a node that is not a master. Lets try another one.'
			host = find_alive_master_nodes()

		# Master returns 0 to signal 'do nothing'
		elif user > 0:

			try:

				# Ask twitter for the user's information
				response = crawl(user)

				try:
					# Return the user's information to the master
					print "Sending response to server"
					json_string = json.dumps(response)
					reply_socket.send(str(len(json_string)))
					
					reply_socket.recv(1024)
					
					reply_socket.send(json_string)
				except:
					print 'There was a server timeout.'

			except urllib2.HTTPError, e:

				# Twitter responds with 'bad request' when rate limit is reached
				if e.code == 400:
					print 'Got status code 400: Rate limit reached.'
					rate_limit_reached = True
				else:
					raise

		print 'just crawled: '+str(user)

	print 'Halting.'
	user_socket.close()
	reply_socket.close()
	return 0


def crawl(user):
	"""
	Gets the followers and followees of a user
	Returns a dictionary with keys user, followers, and followees
	Raises HTTPError if there was an error contacting Twitter

	Keyword arguments:
	user -- the user to crawl
	"""

	response = {'user':user}
	try:
		# Get the followers and followees from twitter
		response['followers'] = request.get_followers(int(user))
		response['followees'] = request.get_followees(int(user))
	except urllib2.HTTPError, e:
		# If the user is private, respond without followers/followees
		if e.code == 401:
			pass
		else:
			raise
	return response

	
def find_alive_nodes():
	backupNodesFile=open('../master/replicate_list', 'r')
	backupNodes = backupNodesFile.read()
	backupNodesFile.close()
	nodelist = backupNodes.splitlines()

	alivenode = 0
	for node in nodelist:
		if ( os.system('ping -q -c1 ' + node) == 0):
			# okay we found a node that is alive
			# TODO: need to add method in server.py to check if node is serving requests
			print 'We found a node that is alive.'
			# gota make sure it's running out code
			try:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((node,CONTROL_PORT))
				alivenode = node # this means a higher priority node is master, lets tell worker thread
				break
			except:
				print 'Node is not running our application.'
			
	return alivenode
	
def find_alive_master_nodes():
	backupNodesFile=open('../master/replicate_list', 'r')
	backupNodes = backupNodesFile.read()
	backupNodesFile.close()
	nodelist = backupNodes.splitlines()

	alivenode = 0
	for node in nodelist:
		if ( os.system('ping -q -c1 ' + node) == 0):
			# okay we found a node that is alive
			# TODO: need to add method in server.py to check if node is serving requests
			print 'We found a node that is alive.'
			try:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((node,CONTROL_PORT))
				print 'Checking if node is master...'
				sock.send('is_master')
				master = sock.recv(1024)
				sock.close()
				if (master == '1'):
					print 'Node is a master!'
					alivenode = node # this means a higher priority node is master, lets tell worker thread
					break
				else:
					print 'Node is NOT a master.'
			except:
				print 'Node is not running a server!'

			
	return alivenode
	
if __name__ == '__main__':
	sys.exit(main(*sys.argv))

