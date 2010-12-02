import socket
import SocketServer
import threading

HOST, PORT = socket.gethostname(), 5630
clients = {}
lock = threading.Lock()

class AnnounceHandler(SocketServer.BaseRequestHandler):
   def handle(self):
      print '***announce received'
      lock.acquire()
      try:
         self.request.send(str(44538169))
         print 'replied announce with user id'

         clients[socket.getfqdn(self.request.getpeername()[0])] = 1
         print 'workers seen:'
         for k in clients:
            print '   ' + k
         print 'unique workers so far: ' + str(len(clients))

      finally:
         lock.release()

if __name__ == '__main__':
   server = SocketServer.TCPServer((HOST, PORT), AnnounceHandler)
   print 'server started at ' + HOST + ':' + str(PORT)
   server.serve_forever()
