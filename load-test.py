import socket
import sys
import xml.etree .cElementTree as ET
from xml.etree.cElementTree import Element, SubElement, XML, fromstring, tostring
from xml.dom import minidom
import random
import string
import time
N = 10000
M = 100
HOST, PORT = sys.argv[1], 12346
def append_zero(str):
    prefix = '0' * (8 - len(str))
    return prefix + str

def convert_to_readable(element):
    string = tostring(element, 'utf-8')
    parsed = minidom.parseString(string)
    return parsed.toprettyxml(indent="    ")
def generate_tags():
    tags = []
    for i in range(M):
        tags.append(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(4)))
    return tags

def recvall(sock):
    BUFF_SIZE = 4096 # 4 KiB
    data = ""
    while True:
        part = sock.recv(BUFF_SIZE)
        data += part
        if part < BUFF_SIZE:
            # either 0 or end of data
            break
    return data

def create_account():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    top = Element('transactions')
    top.set('reset', "true")
    for i in range(N):
        created_node = SubElement(top, 'create')
        if random.randint(0,10) is not 0:
            created_node.set('ref', ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5)))
        account = SubElement(created_node, 'account')
        account.text = str(i)
        if random.randint(0,10) is not 0:
            balance = SubElement(created_node, 'balance')
            balance.text = str(random.randint(0, 1000000))
    payload = convert_to_readable(top)
    try:
        # Connect to server and send data
        sock.connect((HOST, PORT))
        # send_data = str(len(payload)) + payload
        send_data = (append_zero(str(len(payload))) + payload).encode('utf-8')

        print(send_data)
        start_time = time.time()
        sock.sendall(send_data)


        length = sock.recv(8)
        end_time = time.time()
        xml_length = int(length.decode("utf-8"))
        print(xml_length)
        xml = bytes()
        while xml_length > 0:
            msg = sock.recv(4096)
            xml += msg
            xml_length -= len(msg)
        xml_string = str(xml.decode("utf-8"))
        # Receive data from the server and shut down
    finally:
        sock.close()
    print(send_data)
    print(xml_string)
    print("--- %s seconds ---" % (end_time - start_time))

def send_transfer():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    top = Element('transactions')
    top.set('reset', "oiqjeoiwq")
    for i in range(N * 2):
        transfered_node = SubElement(top, 'transfer')
        if random.randint(0,10) is not 0:
            transfered_node.set('ref', ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5)))
        from_tag = SubElement(transfered_node, 'from')
        from_tag.text = str(random.randint(0, N - 1))
        to_tag = SubElement(transfered_node, 'to')
        to_tag.text = str(random.randint(0, N - 1))
        amount_tag = SubElement(transfered_node, 'amount')
        amount_tag.text = str(random.randint(0, 1000000))
        tags_number = random.randint(0, 4)
        for i in range(tags_number):
            tag = SubElement(transfered_node, 'tag')
            tag.text = tags[random.randint(0, M - 1)]
    payload = convert_to_readable(top)
    try:
        # Connect to server and send data
        sock.connect((HOST, PORT))
        # send_data = str(len(payload)) + payload
        send_data = (append_zero(str(len(payload))) + payload).encode('utf-8')

        print(send_data)
        start_time = time.time()
        sock.sendall(send_data)


        length = sock.recv(8)
        end_time = time.time()
        xml_length = int(length.decode("utf-8"))
        print(xml_length)
        xml = bytes()
        while xml_length > 0:
            msg = sock.recv(4096)
            xml += msg
            xml_length -= len(msg)
        xml_string = str(xml.decode("utf-8"))
        # Receive data from the server and shut down
    finally:
        sock.close()
    print(send_data)
    print(xml_string)
    print("--- %s seconds ---" % (end_time - start_time))

tags = generate_tags()
create_account()
send_transfer()
