import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('reala.ece.ubc.ca', 5630))
sock.send(socket.gethostname())
sock.close()
