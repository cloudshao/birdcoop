import csv
import sqlite3
import os

# DB Schema:
# insert into user_table(user_id, name, location, bio) values(1, 'name', 'loc', 'bio')

def import_db():
	conn = sqlite3.connect("/home/usf_ubc_gnutella1/birdcoop/master/awesomeDB2")
	c = conn.cursor()
	#c.execute("create table user_table (user_id integer PRIMARY KEY, name char(20), location char(30), bio char(160))")

	csvReader = csv.reader(open('/home/usf_ubc_gnutella1/birdcoop/master/user_crawled_table.csv'), delimiter=',', quotechar='"')
	for row in csvReader:
		c.execute('replace into user_crawled_table(user_id, crawled, currTime) values(?,?,?)', row)
	conn.commit()

	csvReader = csv.reader(open('/home/usf_ubc_gnutella1/birdcoop/master/tweet_table.csv'), delimiter=',', quotechar='"')
	for row in csvReader:
		c.execute('replace into tweet_table(user_id, time, tweet, currTime) values(?,?,?,?)', row)
	conn.commit()
	
	csvReader = csv.reader(open('/home/usf_ubc_gnutella1/birdcoop/master/user_table.csv'), delimiter=',', quotechar='"')
	for row in csvReader:
		c.execute('replace into user_table(user_id, name, location, bio, currTime) values(?,?,?,?,?)', row)
	conn.commit()
	
	csvReader = csv.reader(open('/home/usf_ubc_gnutella1/birdcoop/master/follower_table.csv'), delimiter=',', quotechar='"')
	for row in csvReader:
		c.execute('replace into follower_table(user_id, follower_id, currTime) values(?,?,?)', row)
	conn.commit()
	
	return

if __name__ == '__main__':
	print "Importing AwesomeDB backup."
	import_db()
	print "AwesomeDB backup imported successfully."
