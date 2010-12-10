import random
import request
import simplejson as json
import socket
import sys
import urllib2

try:
   HOST = sys.argv[1]
except IndexError:
   print 'Usage: python test_worker.py <master>'
   sys.exit(1)

ANNOUNCE_PORT = 5630
REPLY_PORT = 5631

# Crawl this many times before stopping
for i in range(10000):

   # Ask the master for a user to crawl
   sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   sock.connect((HOST, ANNOUNCE_PORT))
   user = sock.recv(1024)
   sock.close()

   # Master returns 0 to say don't crawl anything
   if int(user) == 0:
      continue
   
   # Generate 100 users that both follow and are followed by this user
   followers = []
   followees = []
   for j in range(100):
      temp_u = {}
      temp_u['id'] = random.randint(1, 99999999)
      temp_u['name'] = 'John Doe'
      temp_u['location'] = 'Here'
      temp_u['description'] = 'My name is john doe hello everyone'
      followers.append(temp_u)
      followees.append(temp_u)
   response = json.dumps({'user':user,
                          'followers':followers,
                          'followees':followees,})

   # Send the "crawl" results back to the master
   sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   sock.connect((HOST, REPLY_PORT))
   sock.send(response)
   sock.close()

   print 'total: ' + str(i) + ', just crawled: ' + user
