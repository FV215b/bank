import socket
import sys
import string

HOST, PORT, NUM = sys.argv[1], 12348, 0

def append_zero(str):
    prefix = '0' * (8 - len(str))
    return prefix + str

if __name__ == "__main__":
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    try:    
        filename = 'xml' + str(NUM) + '.txt'
        f = open(filename, 'r')
        payload = f.read()
        send_data = (append_zero(str(len(payload))) + payload).encode('utf-8')
        sock.sendall(send_data)
        length = sock.recv(8)
        print(length)
        xml_length = int(length.decode("utf-8"))
        print(xml_length)
        xml = bytes()
        while xml_length > 0:
            msg = sock.recv(4096)
            xml += msg
            xml_length -= len(msg)
        xml_string = str(xml.decode("utf-8"))
        print(xml_string)
    finally:
        sock.close()

