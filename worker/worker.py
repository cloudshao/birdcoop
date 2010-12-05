import simplejson as json
import socket
import request
import urllib2

HOST = 'reala.ece.ubc.ca'
ANNOUNCE_PORT = 5630
REPLY_PORT = 5631

for i in range(2):
   try:
      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      sock.connect((HOST, ANNOUNCE_PORT))
      user = sock.recv(1024)

      followers = json.loads(request.getFollowersJson(int(user)))
      followees = json.loads(request.getFolloweesJson(int(user)))
      response = json.dumps({'user':user,
                             'followers':followers,
                             'followees':followees,})

      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      sock.connect((HOST, REPLY_PORT))
      sock.send(response)
      sock.close()

      print 'total: ' + str(i) + ', just crawled: ' + user

   except urllib2.HTTPError, e:
      if e.code == 401:
         pass
      else:
         raise

   except socket.error, e:
      print e
