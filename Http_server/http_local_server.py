from http.server import HTTPServer, BaseHTTPRequestHandler

class HandlerClass(BaseHTTPRequestHandler): # определение обработчика запроса
    def do_GET(self):

        self.send_response(200) # код ответа

        self.send_header("Content-type", "text/html") # тип ответа
        self.end_headers() # буфер ->

        self.wfile.write('Hello, World'.encode("utf-8"))



host_name = "0.0.0.0"
server_port = 8000

local_server = HTTPServer((host_name, server_port), HandlerClass)

print(f'launch server http://{host_name}:{server_port}')

try:

    local_server.serve_forever()

except KeyboardInterrupt:

    local_server.server_close()
