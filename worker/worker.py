import request
import simplejson as json
import socket
import sys
import urllib2

ANNOUNCE_PORT = 5630
REPLY_PORT = 5631


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
   while not rate_limit_reached:

      # Get the user to crawl from the master
      user = announce(host, ANNOUNCE_PORT)

      # Master returns 0 to signal 'do nothing'
      if user:

         try:
            # Ask twitter for the user's information
            response = crawl(user)
         except urllib2.HTTPError, e:
            # Twitter responds with 'bad request' when rate limit is reached
            if e.code == 400:
               print 'Got status code 400: Rate limit reached.'
               rate_limit_reached = True
            else:
               raise

         # Return the user's information to the master
         respond(response, host, REPLY_PORT)

      print 'just crawled: '+str(user)

   print 'Halting.'
   return 0


def announce(host, port):
   """
   Connect to the master and receive a user id to crawl

   Keyword arguments:
   host -- the hostname of the master to contact
   port -- the port of the master
   """
   sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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

   response = {'user':user}
   try:
      # Get the followers and followees from twitter
      response['followers'] = request.get_followers(int(user))
      response['followees'] = request.get_followees(int(user))
   except urllib2.HTTPError, e:
      # If the user is private, respond without followers/followees
      if e.code == 401:
         pass
      else:
         raise
   return response


def respond(response, host, port):
   """
   Sends a dictionary object to the master

   Keyword arguments:
   response -- the dict to send
   host -- the hostname of the master
   port -- the master's response port
   """
   sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   sock.connect((host, port))
   sock.send(json.dumps(response))
   sock.close()


if __name__ == '__main__':
   sys.exit(main(*sys.argv))

