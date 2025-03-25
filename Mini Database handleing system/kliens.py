import socket


#Mihaly Zoltan-Zsolt, 523/1
# Define server host and port
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 12345

# Create a TCP/IP socket
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
    # Connect the socket to the server
    try:
        client_socket.connect((SERVER_HOST, SERVER_PORT))
        print(f"Connection to {SERVER_HOST}:{SERVER_PORT} successful.")
    except:
        print(f"Connection to {SERVER_HOST}:{SERVER_PORT} failed.")
        exit(1)
    # Send data to the server
    message = input("Give me a message to send:")
    messageSent = str(message)
    client_socket.sendall(messageSent.encode())
    if messageSent.lower().strip() == 'exit':
        exit(0)  # If "exit" key word is given the server closes
    # Receive data from the server
    data = client_socket.recv(1024)

    print(f"Received from server: {data.decode()}")