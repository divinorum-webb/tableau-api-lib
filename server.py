import http.server
import socketserver

class MyRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.path = '/print.html'
        return http.server.SimpleHTTPRequestHandler.do_GET(self)

Handler = MyRequestHandler
server = socketserver.TCPServer(('0.0.0.0', 8080), Handler)

server.serve_forever()