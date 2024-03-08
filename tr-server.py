"""
Write a server in file tr-server.py, which runs forever and generates a
transaction (an arbitrary string for the purposes of the project) every second.
Make sure that the generated transactions are unique, i.e., no string is repeated.
The transactions are emitted at port 9999.
"""
import time
import string
import random
import socket

print(__doc__)


_PORT = 9999
_HOST = 'localhost'


def generate_string(start=int(1), stop=int(1024)) -> string:
    """ returns a string with length between 1 and 1024 containing printable characters """
    s = ''.join(random.choices(string.ascii_lowercase+string.ascii_uppercase+string.digits, k=random.randrange(start, stop)))
    return s


def register_client():
    """Listens for clients that want to receive the generated quotes"""
    pass


def run_server():
    """Runs the server that generates quotes and sends them to listeners"""
    pass

def main(host=_HOST, port=_PORT):
    # Create a socket object
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Bind the socket to the host and port
        server_socket.bind((host, port))

        # Start listening for incoming connections
        server_socket.listen(5)
        print(f"[*] Listening on {host}:{port}")

        client_socket = None
        address = None
        # while True:
        #     # Accept a client connection
        #     client_socket, address = server_socket.accept()
        #     print(f"[*] Accepted connection from {address[0]}:{address[1]}")
        #     break
        client_socket, address = server_socket.accept()
        print(f"[*] Accepted connection from {address[0]}:{address[1]}")

        while True:
            # Send a message to the client(s)
            message = generate_string()
            client_socket.sendall(message.encode('utf-8'))
            print(f"[*] Sent message to {address[0]}:{address[1]}: {message}")
            # Sleep for 5 seconds
            time.sleep(5)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the client socket
        if None != client_socket:
            client_socket.close()
        # Close the server socket
        server_socket.close()

if __name__ == "__main__":
    main()
