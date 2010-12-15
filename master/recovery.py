import os
import sys
import string
import socket
from socket import gethostname

CONTROL_PORT = 5632

def find_alive_nodes(lastnode):
	backupNodesFile=open('replicate_list', 'r')
	backupNodes = backupNodesFile.read()
	backupNodesFile.close()
	nodelist = backupNodes.splitlines()
	me = gethostname()
	
	alivenode = 0
	for node in nodelist:
		#print me
		#print node
		if (lastnode in node):
			print 'list stopped at me. breaking...'
			break
		elif (me in node):
			print 'this node is ME'
			continue # need to skip ourselves
		elif ( os.system('ping -q -c1 ' + node) == 0):
			# okay we found a node that is alive
			# TODO: need to add method in server.py to check if node is serving requests
			print 'We found a node that is alive.'
			try:
				print 'Checking if node is running a server'
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((node,CONTROL_PORT))
				sock.close()
				alivenode = node # this means a higher priority node is master, lets tell worker thread
				break
			except:
				print 'Node is not running our application.'

	return alivenode


def find_alive_master_nodes(lastnode):
	backupNodesFile=open('replicate_list', 'r')
	backupNodes = backupNodesFile.read()
	backupNodesFile.close()
	nodelist = backupNodes.splitlines()
	me = gethostname()
	
	alivenode = 0
	for node in nodelist:

		if (lastnode in node):
			break
		elif (me in node):
			continue # need to skip ourselves
		elif ( os.system('ping -q -c1 ' + node) == 0):
			# okay we found a node that is alive
			# TODO: need to add method in server.py to check if node is serving requests
			print 'We found a node that is alive.'
			try:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((node,CONTROL_PORT))
				print 'Checking if node is master...'
				sock.send('is_master')
				master = sock.recv(1024)
				print 'rcvd master: '+master
				sock.close()
				if 'True' in master:
					print 'Node is a master!'
					alivenode = node # this means a higher priority node is master, lets tell worker thread
					break
			except Exception, e:
				print e
				print 'Node is not running a server!'

	return alivenode

	
def stop_master_request(master_status):
	# need to stop accepting new requests
	print 'Received a request to stop accepting workers.'
	master_status = False
	print 'Node is no longer master node.'
	return master_status


def handle_master_request(master_status):
	# we enter this function if we receive a request from a work to become the new master	
	# this fetches the hostname of this node
	if not master_status: 
		print 'Received request to become a master.'
		myhostname = gethostname()
		print 'Checking if any higher priority nodes are alive...'
		alive_parent_node = find_alive_nodes(myhostname)
		
		if (alive_parent_node != 0 ):
			print 'Bad worker! There already is an active higher-priority master node!'
			return alive_parent_node, master_status # alright lets just return name of higher node that is alive and serving requests
		
		# if we got this far, then it looks like we gotta become the master!
		# TODO: start up all server-related stuff
		print 'Alright, looks like no higher priority nodes are alive. Time for us become master!'
		master_status = True
	return 1, master_status
	

def stop_current_master(currentMaster):
	# TODO: tell current master to stop requests while we transfer over database
	# print 'TODO: tell currentmaster to stop accepting new requests'
	print 'Found another master. Sending request for master to stop.'
	try:
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((currentMaster,CONTROL_PORT))
		sock.send('stop_master')
		response = sock.recv(1024)
		sock.close()
		print 'Stopped current master.'
	except:
		print 'Timed out trying to stop current master.'
	

def announce_new_master(currentMaster):
	# TODO: tell current master taht we are the new master
	# print 'TODO: tell currentmaster i am new master'
	print 'Time to declare that we are new master node'
	try:
		me = gethostname()
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((currentMaster,CONTROL_PORT))
		sock.send(str('new_master;'+me))
		sock.close()
		print 'We are the new master node!'
	except:
		print 'Timed out while trying to announce new master.'
	
	
def get_fresh_database(currentMaster):
	# we need to SCP awesomeDB from currentMaster to me
	# problem: we cannot initiate an SCP from planetlab -> reala since we don't have a public/private key pairing
	# solution: lets just get the backup from anyone
	print 'Fetching a clean database file from another node'
	try:
		if (currentMaster == 'reala.ece.ubc.ca'):
			myhostname = gethostname()
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((currentMaster,CONTROL_PORT))
			print 'Asking reala to send us the DB.'
			sock.send('send_db;'+myhostname)
			done = sock.recv(1024)
			sock.close()
		else:
			cmd = "scp -i group2@eece411 usf_ubc_gnutella1@"+currentMaster+":~/birdcoop/master/awesomeDB awesomeDB"
			os.system(cmd);
			cmd = "scp -i group2@eece411 usf_ubc_gnutella1@"+currentMaster+":~/birdcoop/master/awesomeDB-journal awesomeDB-journal"
			os.system(cmd);
			print 'Fresh DB fetched successfully.'
	except: 
		print 'Could not transfer DB file.'
		pass
	
	
def regain_master_status(master_status):
	# okay we should only be in this method for one of two reasons:
	# 1. We are REALA, and we just recovered from a failure
	# 2. We are a BACKUP node, and just recovered from a failure, AND no higher priority nodes are alive
	print 'It seems we are the highest priority replica node. We need to regain master status.'
	print 'Finding and stopping current master...'
	currentMaster = find_alive_master_nodes(' ')
	if (currentMaster != 0):
		stop_current_master(currentMaster)
		stop_master_db(currentMaster)
		print 'Fetching a fresh database file to replace our corrupted one'
		get_fresh_database(currentMaster)
	
		print 'Telling everyone that we are new master.'
		announce_new_master(currentMaster)
		print 'We have successfully regained master status!'
		
	# TODO: start up all all master server threads
	print 'Setting our status to "master"'
	master_status = True
	return master_status

def stop_master_db(current_master):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((current_master,CONTROL_PORT))
	print 'Telling master to stop writing to DB while transfering.'
	sock.send('stop_db')
	done = sock.recv(1024)
	sock.close()
	if 'db_stopped' in done:
		return
	else:
		print 'ERROR: db could not be stopped.'
		
def start_master_db(current_master):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((current_master,CONTROL_PORT))
	print 'DB transfer complete. Telling master to start DB writing again.'
	sock.send('start_db')
	sock.close()

def send_db(remote_host):
	try:
		cmd = "scp -i group2@eece411 awesomeDB usf_ubc_gnutella1@"+remote_host+":~/birdcoop/master/awesomeDB"
		os.system(cmd);
		cmd = "scp -i group2@eece411 awesomeDB-journal usf_ubc_gnutella1@"+remote_host+":~/birdcoop/master/awesomeDB-journal"
		os.system(cmd);
		print 'Fresh DB fetched successfully.'
	except:
		print 'Could not transfer DB file.'
		pass
	

def check_and_regain_master(master_status, backup_status):
	# we just recovered from failure
	# this method is a check to see if we SHOULD be master.
	# we should be master if all parent/higher priority nodes are DEAD, or if we're REALA
	myhostname = gethostname()
	
	if not backup_status:
		print 'We are primary master (usually reala) - so we need to regain master status from any node.'
		master_status = regain_master_status(master_status)
	else:
		print 'We are a backup, so lets see if we need to become master.'
		alive_parent_node = find_alive_nodes(myhostname)
		if (alive_parent_node == 0):
			# no parent nodes alive - we need to be master
			print 'There are no parent nodes that are alive. We need to become master.'
			master_status = regain_master_status(master_status)
		else:
			# there is a parent node that's master. lets just get a database
			print 'There is a parent node alive. No need to become master.'
			current_master = find_alive_nodes(' ')
			stop_master_db(current_master)
			get_fresh_database(current_master)
			start_master_db(current_master)
			
	return master_status
	
