import pickle
import socket as socket
import tkinter as tk

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 12345


def send_data_to_server(data):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        try:
            client_socket.connect((SERVER_HOST, SERVER_PORT))
            print(f"Connection to {SERVER_HOST}:{SERVER_PORT} successful.")
        except Exception as e:
            print(f"Connection to {SERVER_HOST}:{SERVER_PORT} failed, due to: {e}")
            exit(1)
        client_socket.sendall(data.encode())
        response = client_socket.recv(10**9)
        if data != "exit":
            text = data.split()
            command = (text[0] + " " + text[1]).upper()
            print(response.decode())
            '''if (response.decode() == f"Command {command} execution failed!" or response.decode() == f"Invalid command!"):
                show_error_message(response.decode())
                return []
            else:'''
            return response.decode()


def show_error_message(message):
    error_window = tk.Toplevel()
    error_window.geometry("300x100")
    error_window.title("Error")

    error_label = tk.Label(error_window, text="Error: " + message)
    error_label.pack()


def fetch_column_names(table_name):
    column_names = send_data_to_server("get columns " + table_name)
    #    print(column_names)
    if column_names is not None:
        return column_names.split()
    else:
        return {}


def receive_folder_structure():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        try:
            client_socket.connect((SERVER_HOST, SERVER_PORT))
            print(f"Connection to {SERVER_HOST}:{SERVER_PORT} successful.")
        except Exception as e:
            print(f"Connection to {SERVER_HOST}:{SERVER_PORT} failed, due to: {e}")
            exit(1)
        client_socket.sendall("get folder ./".encode("utf8"))
        response = client_socket.recv(32768)
        if (response.decode(errors='ignore') == f"Command get folder execution failed!" or response.decode(
                errors='ignore') == f"Invalid command!" or response.decode(errors='ignore') == ''):
            show_error_message(response.decode())
            return []
        else:
            folder_structure = pickle.loads(response)
            return folder_structure
