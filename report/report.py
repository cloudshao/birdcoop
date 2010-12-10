import sqlite3
import sys
import json
import urllib2
from django.utils.encoding import smart_str, smart_unicode

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
	for row in cursor:
		index.write('<tr><td>')
		index.write(row[0])
		index.write('</td><td>')
		index.write(str(row[1]))
		index.write('</td></tr>')		
	index.write('</table><br/><br/>')

def getPopularName(index):
	#select the most popular name 
	print "Getting the most popular name"
	cursor.execute('select name, count(*) from user_table group by name having count(*) order by count(*) desc limit 5')
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
	cursor.execute('select follower_id from follower_table desc limit 5000')
	for row in cursor:
		cursor2.execute("select count(*) from follower_table where user_id='%s'" %row[0])
		cursor3.execute("select count(*) from follower_table where follower_id='%s'" %row[0])
		countUsers =  int(cursor2.fetchone()[0])
		countFollowers = int(cursor3.fetchone()[0])
		if (countUsers+50 < countFollowers):
			bots = bots+1
		if (countUsers > countFollowers+50):
			celebrities = celebrities+1
		totalUsers = totalUsers+1
	index.write('Ratio of Bots and Celebrities <br/> Bots have many more following than followers <br/> Celebrities have many more followers than people followed <br/>')
	index.write('<table border = 1>')
	index.write('<tr><td>Bots</td><td>')
	index.write(str(bots))
	index.write('<tr><td>Celebrities</td><td>')
	index.write(str(celebrities))
	index.write('<tr><td>totalUsers</td><td>')
	index.write(str(totalUsers))
	index.write('</td></tr></table><br/><br/>')
	#print bots
	#print celebrities
	#print totalUsers		

def getCommonLocations(index):
	location = []
	index.write('Trend of locations of Twitter<br/>')
	index.write('<table border = 1>')
	index.write('<tr><td>Start</td><td>Followed</td></tr>')
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
	index.write('<html><head><title>Twitter results</title></head><body><h3>Twitter Results </h3>')
	return
   
def buildFooter(index):
	index.write('</body></html>')
	return

#Export text "visuals" to a text file and upload to html page
index = open('index.html', 'w')
buildHeader(index)
conn = sqlite3.connect("awesomeDB")
cursor = conn.cursor()
cursor2 = conn.cursor() 
cursor3 = conn.cursor()
getPopularName(index)
getBots(index)
getCommonLocations(index)
cursor.close()
cursor2.close()
cursor3.close()
buildFooter(index)
index.close()
