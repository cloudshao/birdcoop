import sqlite3
import sys
import json
import urllib2

def most_common(lst):
	return max(set(lst), key=lst.count)
		
def insertCol(value, index):
   index.write('<td>')
   index.write(value)
   index.write('</td>')

def makePopNameTable(cursor, index):
	#Add table to html
	index.write('Most Common Name </br>')
	index.write('<table border = 1>')
	insertCol('Name', index)
	insertCol('Occurrence', index)
	names = []
	freq = []
	for row in cursor:
		index.write('<tr><td>')
		try:
			index.write(row[0])
			names.append(row[0])
		except:
			index.write("Cannot write name")
			names.append("Invalid name")
		index.write('</td><td>')
		index.write(str(row[1]))
		freq.append(str(row[1]))
		index.write('</td></tr>')		
	index.write('</table><br/>')
	
	script = '<script type="text/javascript"> graph = new BAR_GRAPH("hBar"); graph.values = "' + freq[0] + ',' + freq[1] + ',' + freq[2] + ',' + freq[3] + ',' + freq[4] + ',' + freq[5] + ',' + freq[6] + ',' + freq[7] + ',' + freq[8] + ',' + freq[9] + '"; graph.labels = "' + names[0] + ',' + names[1] + ',' + names[2] + ',' + names[3] + ',' + names[4] + ',' + names[5] + ',' + names[6] + ',' + names[7] + ',' + names[8] + ',' + names[9] + '"; document.write(graph.create()); </script><br/><br/>'
	index.write(script)

def getPopularName(index):
	#select the most popular name 
	print "Getting the most popular name from pool of 20,000 randomly chosen users"
	cursor.execute('select name, count(*) from user_table group by name having count(*) order by count(*) desc limit 10')
	makePopNameTable(cursor, index)
	firstOne = []
	firstTwo = []
	last = []
	count = 0
	cursor.execute('select name from user_table desc limit 20000')
	for row in cursor:
		name = row[0].rsplit(' ')
		if (name[0] != 'The'):
				firstOne.append(name[0])
	commonFirst = most_common(firstOne)
	cursor.execute('select name from user_table desc limit 20000')
	for row in cursor:
		last.append(name[len(name)-1])
	commonLast = most_common(last)
	#index.write('<table style="border-width: 1px; border-color: #000000; border-style: none; border-spacing: 15px; text-align: center; font-family: Verdana; font-size: 10px;">')
	index.write('Most Common First and Last Name <br/>')
	index.write('<table border = 1>')
	index.write('<tr><td>First</td><td>')
	try:
		index.write(commonFirst)
	except:
		index.write('Cannot write name')
	index.write('<tr><td>Last</td><td>')
	#commonLast = "test"
	try:
		index.write(commonLast)
	except:
		index.write('Cannot write name')
	index.write('</td></tr></table><br/><br/>')


def getBots(index):
	print "To be finished"
	bots = 0
	celebrities = 0
	totalUsers = 0
	used = 0
	toCrawl = 100
	coolness = [[0 for col in range(3)] for row in range(toCrawl)]
	cursor.execute('select follower_id from follower_table desc limit 5000')
	for row in cursor:
		cursor2.execute("select count(*) from follower_table where user_id='%s'" %row[0])
		cursor3.execute("select count(*) from follower_table where follower_id='%s'" %row[0])
		countUsers =  int(cursor2.fetchone()[0])
		countFollowers = int(cursor3.fetchone()[0])
		coolness.sort()
#		for i in range(10):
#		  if coolness[0][1] == row[0] or coolness[1][1] == row[0] or coolness[2][1] == row[0] or coolness[3][1] == row[0] or coolness[4][1] == row[0] or coolness[5][1] == row[0] or coolness[6][1] == row[0] or coolness[7][1] == row[0] or coolness[8][1] == row[0] or coolness[9][1] == row[0]:
#			  break
#		  if coolness[i][0] < countFollowers:
#		    coolness[i][0] = countFollowers
#		    coolness[i][1] = row[0]
#		    break
		for i in range(toCrawl):
		  used = 0
		  for y in range(toCrawl):
		 	  if coolness[y][1] == row[0]:
		 	    used=1
		 	    break
		  if coolness[i][0] < countFollowers and used == 0:
		 	  coolness[i][0] = countFollowers
		 	  coolness[i][1] = row[0]
		if (countUsers+50 < countFollowers):
			bots = bots+1
		if (countUsers > countFollowers+50):
			celebrities = celebrities+1
		totalUsers = totalUsers+1

	for i in range(toCrawl):
	  coolness[i][2] = coolness[i][0]
	  for y in range(toCrawl):
	    sql2 = "select follower_id from follower_table where user_id = '%s' and follower_id = '%s'" % (coolness[i][1], coolness[y][1])
	    #sql2 = "select follower_id from follower_table where user_id = 216393728 and follower_id = 219831583"
	    cursor2.execute(sql2)
	    sql3 = "select follower_id from follower_table where user_id = '%s' and follower_id = '%s'" % (coolness[y][1], coolness[i][1])
	    #sql3 = "select follower_id from follower_table where user_id = 219831583 and follower_id = 216393728"
	    cursor3.execute(sql3)
	    #temp = cursor2.fetchone()
	    #temp2 = cursor3.fetchone()
	    if cursor2.fetchone() and cursor3.fetchone() is None: #and cursor3.fetchone() is False:
	      coolness[i][2] = (coolness[y][0])/2 + coolness[i][2]
	print coolness

	index.write('Ratio of Bots and Celebrities <br/> Bots have many more following than followers <br/> Celebrities have many more followers than people followed <br/>')
	index.write('<table border = 1>')
	index.write('<tr><td>Bots</td><td>')
	index.write(str(bots))
	index.write('<tr><td>Celebrities</td><td>')
	index.write(str(celebrities))
	index.write('<tr><td>Other Users</td><td>')
	index.write(str(totalUsers-bots-celebrities))
	index.write('</td></tr></table><br/>')
	script = '<script type="text/javascript"> graph = new BAR_GRAPH("hBar"); graph.values = "' + str(bots) + ',' + str(celebrities) + ',' + str(totalUsers) + '"; graph.labels = "Bots, Celebrities, Total Users"; document.write(graph.create()); </script><br/><br/>'
	
	
	index.write(script)
	#print bots
	#print celebrities
	#print totalUsers		

def getCommonLocations(index):
	location = []
	index.write('Trend of locations of Twitter<br/>')
	index.write('<table border = 1>')
	index.write('<tr><td>Followees</td><td>Followed</td></tr>')
	print "getting locations"
	cursor.execute('select location, count(*) from user_table group by location having count(*) order by count(*) desc limit 10')
	for row in cursor:
		if (row[0]):
			cursor2.execute("select user_id from user_table where location='%s'" %row[0])
			print "People in city:  " + row[0]
			index.write('<tr><td>')
			try:
				index.write(row[0])
			except:
				index.write('Cannot write name')
			index.write('</td><td>')
			for row2 in cursor2:
				cursor3.execute("select location from user_table where user_id = (select follower_id from follower_table where user_id='%s') order by count(*) desc limit 1" %row2[0])
				for row3 in cursor3:
					if (row3[0] and row3[0] != row[0]):
						location.append(row3[0])
			print "Followed by:  " + most_common(location)
			try:
				index.write(most_common(location))
			except:
				index.write('Cannot write name')
			#index.write(most_common(location))
			index.write('</td></tr>')
	index.write('</table><br/><br/>')

def buildHeader(index):
	index.write('<html><head><title>Twitter results</title></head><body><h3>Twitter Results </h3> <script type="text/javascript" src="graphs.js"></script>')
	return
   
def buildFooter(index):
	index.write('</body></html>')
	return

#Export text "visuals" to a text file and upload to html page
index = open('index.html', 'w')
buildHeader(index)
conn = sqlite3.connect("awesomeDB")
conn.text_factory = str
cursor = conn.cursor()
cursor2 = conn.cursor() 
cursor3 = conn.cursor()
#getPopularName(index)
getBots(index)
#getCommonLocations(index)
cursor.close()
cursor2.close()
cursor3.close()
buildFooter(index)
index.close()
