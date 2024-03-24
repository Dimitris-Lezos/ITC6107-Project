"""
Write a server in file tr-server.py, which runs forever and generates a
transaction (an arbitrary string for the purposes of the project) every second.
Make sure that the generated transactions are unique, i.e., no string is repeated.
The transactions are emitted at port 9999.
"""
import datetime
import threading
import time
import string
import random
import socket
from parameters import _HOST, _PORT

print(__doc__)




# the registered clients
client_sockets = list()


def generate_message(start=int(1), stop=int(64)) -> string:
    """
    returns a string with length between start and stop containing printable characters, prepended by the current datetime
    :param start: minimum length of the random part of the string
    :param stop: maximum length of the random part of the string
    :return: a string with length between start and stop containing printable characters, prepended by the current datetime
    """
    now = datetime.datetime.now()
    s = str(now) + ' ' + ''.join(random.choices(string.ascii_lowercase+string.ascii_uppercase+string.digits, k=random.randrange(start, stop)))
    return s+'\n'


def register_client(host, port) -> None:
    """
    Listens for clients that want to receive the generated quotes
    :param self: self
    :param host: The host to listen to
    :param port: The port to listen to
    :return: None
    """
    # Create a socket object
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Bind the socket to the host and port
        server_socket.bind((host, port))
        # Start listening for incoming connections
        server_socket.listen()
        print(f"[*] Listening on {host}:{port}")
        client_socket = None
        while True:
            # Accept a client connection
            client_socket, address = server_socket.accept()
            print(f"[*] Accepted connection from {address[0]}:{address[1]}")
            client_sockets.append((client_socket, address))
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the client sockets
        for client_socket, address in client_sockets:
            client_socket.close()
        # Close the server socket
        server_socket.close()


def run_server(host=_HOST, port=_PORT) -> None:
    """
    Runs a string generator that listens to host:port for clients to which it sends the generated string.
    :param host: the host to listen to
    :param port: the port to listen to
    :return: None
    """
    # Create a thread that listens for connections
    listener = threading.Thread(target=register_client, args=[host, port])
    listener.start()
    # Generate strings and send them to clients
    while True:
        bad_sockets = list()
        # Send a message to the client(s)
        message = generate_message()
        print("Generated message: ", message)
        for client_socket, address in client_sockets:
            try:
                client_socket.sendall(message.encode('utf-8'))
                print(f"[*] Sent message to {address[0]}:{address[1]}: {message}")
            except OSError as e:
                print(f"[*] FAILED to sent message to {address[0]}:{address[1]}")
                bad_sockets.append((client_socket, address))
        # Remove any sockets that had failures
        for bad_socket in bad_sockets:
            try:
                client_sockets.remove(bad_socket)
                client_socket, address = bad_socket
                print(f"[*] Removed bad socket from {address[0]}:{address[1]}")
            except OSError as e:
                pass
        # Sleep for 0.1 up to 4.0 secs with a mean of 1 seconds
        time.sleep(min(4.0, max(0.1, random.normalvariate(1.0, 2.0))))

if __name__ == "__main__":
    run_server()
