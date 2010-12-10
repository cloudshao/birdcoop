import csv
import sqlite3
import os
import os.path
import time

def backup_db(oldTimestamp):
	conn = sqlite3.connect("awesomeDB2")
	cursor = conn.cursor()
	
	# gotta get each table:
	selectCommand = "select * from user_crawled_table where currTime > '"+oldTimestamp+"';"
	cursor.execute(selectCommand)
	csv_writer = csv.writer(open("user_crawled_table.csv", "wt"))
	csv_writer.writerows(cursor)
	
	selectCommand = "select * from tweet_table where currTime > '"+oldTimestamp+"';"
	cursor.execute(selectCommand)
	csv_writer = csv.writer(open("tweet_table.csv", "wt"))
	csv_writer.writerows(cursor)
	
	selectCommand = "select * from user_table where currTime > '"+oldTimestamp+"';"
	cursor.execute(selectCommand)
	csv_writer = csv.writer(open("user_table.csv", "wt"))
	csv_writer.writerows(cursor)
	
	selectCommand = "select * from follower_table where currTime > '"+oldTimestamp+"';"
	cursor.execute(selectCommand)
	csv_writer = csv.writer(open("follower_table.csv", "wt"))
	csv_writer.writerows(cursor)
	
	# we don't actually need this anymore: csv_writer.writerow([i[0] for i in cursor.description]) # write headers
	del csv_writer # this will close the CSV file
	return

def replicate_db(nodeList):
	for node in nodeList:
		print "Transfering backup to replica server: "+node
		cmd = "ssh -n -i group2@eece411 usf_ubc_gnutella1@"+node+" 'sudo chown -R usf_ubc_gnutella1:root ~/birdcoop'"
		os.system(cmd);
		cmd = "scp -i group2@eece411 user_crawled_table.csv tweet_table.csv user_table.csv follower_table.csv import_awesomebackup.py usf_ubc_gnutella1@"+node+":~/birdcoop/master"
		os.system(cmd);
		cmd = "ssh -n -i group2@eece411 usf_ubc_gnutella1@"+node+" 'python ~/birdcoop/master/import_awesomebackup.py'"
		os.system(cmd)
	return
	
def build_replist():
	rList = []
	counter = 0
	nodes=open('replicate_list', 'r')
	node = nodes.read()
	nodes.close()
	rList = node.splitlines()
	return rList
	
def getandset_timestamp(new_timestamp):
	try:
		ts_file = open('last_backup_time', 'r')
		lastTimestamp = ts_file.read()
		ts_file.close()
		
	except IOError:
		lastTimestamp = time.time()
	
	ts_file = open('last_backup_time', 'w')
	ts_file.write(str(new_timestamp))
	ts_file.close()
	
	return lastTimestamp
	
	
if __name__ == '__main__':
	print "Initializing list of replica servers."
	repList = build_replist()
	newTimestamp = time.time()
	oldTimestamp = getandset_timestamp(newTimestamp)
	print "Replica list initialized. Backing up AwesomeDB."
	backup_db(oldTimestamp)
	print "AwesomeDB backed up successfully. Transfering backup to replicas."
	replicate_db(repList)
	print "Replicating complete."
	