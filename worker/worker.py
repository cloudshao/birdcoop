import request
#import dummy_request as request
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
	except IndexError:
		print 'Usage: python test_worker.py <master>'
		return 1

	# Keep processing until rate limit is reached
	rate_limit_reached = False
	master_signaled_halt = False
	while not rate_limit_reached and not master_signaled_halt:

		print 'Current host: '+str(host)
		try:
			# Get the user to crawl from the master
			print 'Getting user to crawl...'
			user = announce(host, ANNOUNCE_PORT)	
			print 'Got user '+str(user)
		except:
			user = -1
			print 'Received exception. Looking for another host.'
			host = find_alive_master_nodes()
			while True:
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
			# Ask twitter for the user's information
			response,limit_reached = crawl(user)
			if (limit_reached):
				rate_limit_reached = True
			try:
				# Return the user's information to the master
				respond(response, host, REPLY_PORT)
			except:
				print 'There was a server timeout.'

		# Master returns 0 to signal 'do nothing'
		elif user == 0:
			master_signaled_halt = True

		print 'just crawled: '+str(user)
		
	print 'Halting.'
	return 0


def announce(host, port):
	"""
	Connect to the master and receive a user id to crawl
	Connect to the master and receive a user id to crawl

	Keyword arguments:
	host -- the hostname of the master to contact
	port -- the port of the master
	"""
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.settimeout(30)
	sock.connect((host, port))
	user = int(sock.recv(1024))
	sock.close()
	return user


def crawl(user):
	"""
	Gets the followers and followees of a user
	Returns a dictionary with keys user, followers, and followees
	Raises HTTPError if there was an error contacting Twitter

	Keyword arguments:
	user -- the user to crawl
	"""
	global node_api_count
	
	limit_reached = False
	response = {'user':user}
	try:
		# Get the followers and followees from twitter
		response['followers'],limit_reached = request.get_followers(int(user))
		if not limit_reached:
			response['followees'],limit_reached = request.get_followees(int(user))
	except urllib2.HTTPError, e:
		# If the user is private, respond without followers/followees
		if e.code == 401:
			pass
		else:
			raise
	return response,limit_reached


def respond(response, host, port):
	"""
	Sends a dictionary object to the master

	Keyword arguments:
	response -- the dict to send
	host -- the hostname of the master
	port -- the master's response port
	"""
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	#sock.settimeout(30)
	sock.connect((host, port))
	#print 'JSON dump: '+str(json.dumps(response))
	sock.send(json.dumps(response))
	sock.close()

def find_alive_nodes():
	backupNodesFile=open('../master/replicate_list', 'r')
	backupNodes = backupNodesFile.read()
	backupNodesFile.close()
	nodelist = backupNodes.splitlines()

	alivenode = 0
	for node in nodelist:
		if ( os.system('ping -q -c1 ' + node + ' &> /dev/null') == 0):
			# okay we found a node that is alive
			# TODO: need to add method in server.py to check if node is serving requests
			print 'Node '+str(node)+' is alive.'
			# gota make sure it's running our code
			try:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((node,CONTROL_PORT))
				alivenode = node # this means a higher priority node is master, lets tell worker thread
				break
			except:
				print  str(node)+' is not running a server!'
			
	return alivenode
	
def find_alive_master_nodes():
	backupNodesFile=open('../master/replicate_list', 'r')
	backupNodes = backupNodesFile.read()
	backupNodesFile.close()
	nodelist = backupNodes.splitlines()

	alivenode = 0
	for node in nodelist:
		if ( os.system('ping -q -c1 ' + node + ' &> /dev/null') == 0):
			# okay we found a node that is alive
			# TODO: need to add method in server.py to check if node is serving requests
			print 'Node '+str(node)+' is alive.'
			try:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((node,CONTROL_PORT))
				print 'Checking if '+str(node)+' is master...'
				sock.send('is_master')
				master = sock.recv(1024)
				sock.close()
				if 'True' in master:
					print  str(node)+' is a master!'
					alivenode = node # this means a higher priority node is master, lets tell worker thread
					break
				else:
					print  str(node)+' is NOT a master.'
			except:
				print  str(node)+' is not running a server!'

			
	return alivenode
	
	
if __name__ == '__main__':
	sys.exit(main(*sys.argv))

