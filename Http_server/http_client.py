import requests
#from http_local_server import host_name, server_port

host_name = "server" 
server_port = 8000

address_server = f'http://{host_name}:{server_port}'

response = requests.get(address_server)

if response.status_code != 200:
    print(f"Response code {response.status_code} ")


print(response.text)
