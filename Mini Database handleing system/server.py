import socket
import re
import pickle

from server_files.data_operations import *
from server_files.database_operations import *
from server_files.database_structure import *
from server_files.table_operations import *

HOST = '127.0.0.1'
PORT = 12345

def command_matching(data):
    command_patterns = [
        (r'use\s+(\w+)', 'use'),
        (r'drop\s+database\s+(\w+)', 'delete_database'),
        (r'create\s+database\s+(\w+)', 'create_database'),
        (r'create\s+table\s+(\w+)\s+(.*)', 'create_table'),
        (r'drop\s+table\s+(\w+)', 'delete_table'),
        (r'create\s+index\s+(\w*)\s*on\s+(\w+)\s*(.*)', 'create_index'),
        (r'insert\s+into\s+(\w+)\s+(.*)', 'insert_into_table'),
        (r'delete\s+from\s+(\w+)\s+(.*)', 'delete_from_table'),
        (r'update\s+(\w+)\s+(.*)', 'update_table'),
        (r'select\s+(.*)\s+from\s+(.*\s+where .*)', 'select_from'),
        (r'select\s+(.*)\s+from\s+(.*)', 'select_from'),
        (r'get\s+folder\s+(\S+)', 'get_folder_structure'),
        (r'get\s+columns\s+(\S+)', 'get_table_columns'),
        (r'get\s+database_tables', 'load_table_names')
    ]

    for pattern, command_type in command_patterns:
        match = re.match(pattern, data.lower())
        if match:
            argument_value = match.groups()
            return command_type, argument_value

    return None, None

def handle_client(client_socket, addr):
    with client_socket:
        print(f"Connection from {addr} established.")

        while True:
            try:
                data = b''
                while True:
                    part = client_socket.recv(1024)
                    data += part
                    if len(part) < 1024:
                        break
                if not data:
                    break
            except socket.error:
                print(f"Client receiving error on {client_socket}:{addr}")
                return

            decoded_data = data.decode().strip()

            if decoded_data.lower() == 'exit':
                client_socket.close()
                #exit(0)
                return 0

            command_handle, arguments = command_matching(decoded_data)
            if command_handle is None:
                client_socket.sendall("Invalid command!".encode())
                continue

            try:
                command = globals()[command_handle]
                status = command(*arguments)

                if command_handle == "get_folder_structure":
                    folder_structure = status
                    serialized_structure = pickle.dumps(folder_structure)
                    client_socket.sendall(serialized_structure)
                    continue
                if command_handle == "get_table_columns":
                    column_list = status
                    client_socket.sendall(' '.join(column_list).encode())
                    continue
                if command_handle == "load_table_names":
                    table_list = status
                    client_socket.sendall(' '.join(table_list).encode())
                    continue
            except Exception as e:
                print(f"Error executing command: {e}")
                status = -1

            response = f"Command {command_handle.upper()}"
            if status == 0:
                client_socket.sendall(f"{response} executed successfully!".encode())
            elif status == 1:
                client_socket.sendall(f"{response} execution failed!".encode())
            elif status == 2:
                client_socket.sendall(f"No primary key detected".encode())
            elif status == 3:
                client_socket.sendall(f"Not correct typing".encode())
            elif status == 4:
                client_socket.sendall(f"Unhandled typing".encode())
            elif status is not int and status != -1:
                rsp = ""
                for i in status:
                    rsp += " ".join(i)+"\n"
                client_socket.sendall(rsp.encode())
            else:
                client_socket.sendall(f"Invalid command!".encode())

        print(f"Connection from {addr} closed.")

def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen(5)
        print(f"Server listening on {HOST}:{PORT}...")

        while True:
            try:
                client_socket, addr = server_socket.accept()
                handle_client(client_socket, addr)
            except socket.error:
                print(f"Client connection severed on {client_socket}:{addr}")

if __name__ == "__main__":
    main()
