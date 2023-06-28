openssl req -x509 -newkey rsa:4096 -nodes -keyout server.key -subj /CN=localhost/C=US/ST=CA/L=SanJose/O=Cisco -out server.crt
openssl req -x509 -newkey rsa:4096 -nodes -keyout client.key -subj /CN=localhost/C=US/ST=CA/L=SanJose/O=Cisco -out client.crt
