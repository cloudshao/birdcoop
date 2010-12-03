import csv
import sqlite3
import os

def import_db():
	conn = sqlite3.connect("awesome_rep.db")
	c = conn.cursor()
	c.execute("create table user_table (user_id integer PRIMARY KEY, name char(20), location char(30), bio char(160))")

	csvReader = csv.reader(open('dbcopy.csv'), delimiter=',', quotechar='"')
	for row in csvReader:
		c.execute('insert into user_table(user_id, name, location, bio) values(?,?,?,?)', row)
		
	conn.commit()
	return

if __name__ == '__main__':
	print "Hello World"
	print "Importing DB"
	import_db()
	print "DB Imported successfully."

#insert into user_table(user_id, name, location, bio) values(1, 'name', 'loc', 'bio')