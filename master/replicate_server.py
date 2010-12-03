import csv
import sqlite3
import os

#rList = ['planetlab1.cs.ucla.edu', 'planetlab1.cs.uiuc.edu', 'planetlab1.cs.umass.edu', 'planetlab1.cs.umb.edu', 'planetlab1.cs.uml.edu', 'planetlab1.cs.unb.ca', 'planetlab1.cs.unb.ca', 'planetlab1.csail.mit.edu']
	
def dump_db():
	conn = sqlite3.connect("awesome.db")
	cursor = conn.cursor()
	cursor.execute("select * from user_table;")

	csv_writer = csv.writer(open("dbcopy.csv", "wt"))
	csv_writer.writerow([i[0] for i in cursor.description]) # write headers
	csv_writer.writerows(cursor)
	del csv_writer # this will close the CSV file
	return

def build_rList():
	rList = []
	counter = 0
	nodes=open('replicate_list', 'r')
	node = nodes.readline()
	while (node != ''):
		rList.append(node)
		print rList[counter]
		counter = counter + 1
		node = nodes.readline()
	return rList

if __name__ == '__main__':
	print "Hello World"
	print "Building rep list"
	build_rList()
	print "Rep List Built. Dumping DB."
	dump_db()
	print "DB Dumped successfully."
		