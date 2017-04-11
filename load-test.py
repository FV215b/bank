import socket
import sys
import xml.etree .cElementTree as ET
from xml.etree.cElementTree import Element, SubElement, XML, fromstring, tostring
from xml.dom import minidom
import random
import string
import time
import threading

N = 20
M = 10
HOST, PORT = sys.argv[1], 12346
logicals = ["or", "and", "not"]
relations = ["equals", "less", "greater"]

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

        # print(send_data)
        sock.sendall(send_data)


        length = sock.recv(8)
        xml_length = int(length.decode("utf-8"))
        # print(xml_length)
        xml = bytes()
        while xml_length > 0:
            msg = sock.recv(4096)
            xml += msg
            xml_length -= len(msg)
        xml_string = str(xml.decode("utf-8"))
        # Receive data from the server and shut down
    finally:
        sock.close()
    # print(send_data)
    # print(xml_string)

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

        # print(send_data)
        start_time = time.time()
        sock.sendall(send_data)


        length = sock.recv(8)
        end_time = time.time()
        # print(length.decode("utf-8"))
        xml_length = int(length.decode("utf-8"))
        # print(xml_length)
        xml = bytes()
        while xml_length > 0:
            msg = sock.recv(4096)
            xml += msg
            xml_length -= len(msg)
        xml_string = str(xml.decode("utf-8"))
        # Receive data from the server and shut down
    finally:
        sock.close()
    # print(send_data)
    # print(xml_string)
    print("--- %s seconds ---" % (end_time - start_time))

def send_balance():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    top = Element('transactions')
    top.set('reset', "oiqjeoiwq")
    for i in range(N / 2):
        transfered_node = SubElement(top, 'balance')
        if random.randint(0,10) is not 0:
            transfered_node.set('ref', ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5)))
        account = SubElement(transfered_node, 'account')
        account.text = str(random.randint(0, N - 1))
    payload = convert_to_readable(top)
    try:
        # Connect to server and send data
        sock.connect((HOST, PORT))
        # send_data = str(len(payload)) + payload
        send_data = (append_zero(str(len(payload))) + payload).encode('utf-8')

        # print(send_data)
        start_time = time.time()
        sock.sendall(send_data)


        length = sock.recv(8)
        end_time = time.time()
        # print(length.decode("utf-8"))
        xml_length = int(length.decode("utf-8"))
        # print(xml_length)
        xml = bytes()
        while xml_length > 0:
            msg = sock.recv(4096)
            xml += msg
            xml_length -= len(msg)
        xml_string = str(xml.decode("utf-8"))
        # Receive data from the server and shut down
    finally:
        sock.close()
    # print(send_data)
    # print(xml_string)
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

        # print(send_data)
        start_time = time.time()
        sock.sendall(send_data)


        length = sock.recv(8)
        end_time = time.time()
        # print(length.decode("utf-8"))
        xml_length = int(length.decode("utf-8"))
        # print(xml_length)
        xml = bytes()
        while xml_length > 0:
            msg = sock.recv(4096)
            xml += msg
            xml_length -= len(msg)
        xml_string = str(xml.decode("utf-8"))
        # Receive data from the server and shut down
    finally:
        sock.close()
    # print(send_data)
    # print(xml_string)
    print("--- %s seconds ---" % (end_time - start_time))

def send_query():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    top = Element('transactions')
    top.set('reset', "oiqjeoiwq")
    for i in range(1000):
        query_node = SubElement(top, 'query')
        if random.randint(0,10) is not 0:
            query_node.set('ref', ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5)))
        random_number = random.randint(0, 4)
        if random_number == 0: #do the logical
            choose_logic = random.randint(0, 2)
            logic_node = SubElement(query_node, logicals[choose_logic])
            restric_num = random.randint(0, 2)
            for i in range(restric_num):
                random_number1 = random.randint(0, 1)
                if random_number1 == 0:
                    tag_node = SubElement(logic_node, 'tag')
                    tag_node.set("info", tags[random.randint(0, M - 1)])
                elif random_number1 == 1:
                    choose_query = random.randint(0, 2)
                    if choose_query == 0:
                        to_acc = SubElement(logic_node, 'equals')
                        to_acc.set('to', str(random.randint(0, N - 1)))
                        from_acc = SubElement(logic_node, 'equals')
                        from_acc.set('from', str(random.randint(0, N - 1)))
                    elif choose_query == 1:
                        greater_amount = SubElement(logic_node, 'greater')
                        greater_amount.set('amount', str(random.randint(0, 1000000)))
                    elif choose_query == 2:
                        less_amount = SubElement(logic_node, 'less')
                        less_amount.set('amount', str(random.randint(0, 1000000)))
        elif random_number == 1: #do tag search
            tag_node = SubElement(query_node, 'tag')
            tag_node.set("info", tags[random.randint(0, M - 1)])
        else: #do the relations
            choose_query = random.randint(0, 2)
            if choose_query == 0:
                to_acc = SubElement(query_node, 'equals')
                to_acc.set('to', str(random.randint(0, N - 1)))
                from_acc = SubElement(query_node, 'equals')
                from_acc.set('from', str(random.randint(0, N - 1)))
            elif choose_query == 1:
                greater_amount = SubElement(query_node, 'greater')
                greater_amount.set('amount', str(random.randint(0, 1000000)))
            elif choose_query == 2:
                less_amount = SubElement(query_node, 'less')
                less_amount.set('amount', str(random.randint(0, 1000000)))

    payload = convert_to_readable(top)
    try:
        # Connect to server and send data
        sock.connect((HOST, PORT))
        # send_data = str(len(payload)) + payload
        send_data = (append_zero(str(len(payload))) + payload).encode('utf-8')

        # print(send_data)
        start_time = time.time()
        sock.sendall(send_data)


        length = sock.recv(8)
        end_time = time.time()
        # print(length.decode("utf-8"))
        xml_length = int(length.decode("utf-8"))
        # print(xml_length)
        xml = bytes()
        while xml_length > 0:
            msg = sock.recv(4096)
            xml += msg
            xml_length -= len(msg)
        xml_string = str(xml.decode("utf-8"))
        # Receive data from the server and shut down
    finally:
        sock.close()
    # print(send_data)
    # print(xml_string)
    print("--- %s seconds ---" % (end_time - start_time))

def send_mix():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    top = Element('transactions')
    for i in range(N):
        choose_one_tag = random.randint(0, 3)
        if choose_one_tag == 0:
            query_node = SubElement(top, 'query')
            if random.randint(0,10) is not 0:
                query_node.set('ref', ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5)))
            random_number = random.randint(0, 4)
            if random_number == 0: #do the logical
                choose_logic = random.randint(0, 2)
                logic_node = SubElement(query_node, logicals[choose_logic])
                restric_num = random.randint(0, 2)
                for i in range(restric_num):
                    random_number1 = random.randint(0, 1)
                    if random_number1 == 0:
                        tag_node = SubElement(logic_node, 'tag')
                        tag_node.set("info", tags[random.randint(0, M - 1)])
                    elif random_number1 == 1:
                        choose_query = random.randint(0, 2)
                        if choose_query == 0:
                            to_acc = SubElement(logic_node, 'equals')
                            to_acc.set('to', str(random.randint(0, N - 1)))
                            from_acc = SubElement(logic_node, 'equals')
                            from_acc.set('from', str(random.randint(0, N - 1)))
                        elif choose_query == 1:
                            greater_amount = SubElement(logic_node, 'greater')
                            greater_amount.set('amount', str(random.randint(0, 1000000)))
                        elif choose_query == 2:
                            less_amount = SubElement(logic_node, 'less')
                            less_amount.set('amount', str(random.randint(0, 1000000)))
            elif random_number == 1: #do tag search
                tag_node = SubElement(query_node, 'tag')
                tag_node.set("info", tags[random.randint(0, M - 1)])
            else: #do the relations
                choose_query = random.randint(0, 2)
                if choose_query == 0:
                    to_acc = SubElement(query_node, 'equals')
                    to_acc.set('to', str(random.randint(0, N - 1)))
                    from_acc = SubElement(query_node, 'equals')
                    from_acc.set('from', str(random.randint(0, N - 1)))
                elif choose_query == 1:
                    greater_amount = SubElement(query_node, 'greater')
                    greater_amount.set('amount', str(random.randint(0, 1000000)))
                elif choose_query == 2:
                    less_amount = SubElement(query_node, 'less')
                    less_amount.set('amount', str(random.randint(0, 1000000)))
        elif choose_one_tag == 1:
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
        else:
            query_node = SubElement(top, 'query')
            if random.randint(0,10) is not 0:
                query_node.set('ref', ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5)))
            random_number = random.randint(0, 4)
            if random_number == 0: #do the logical
                choose_logic = random.randint(0, 2)
                logic_node = SubElement(query_node, logicals[choose_logic])
                restric_num = random.randint(0, 2)
                for i in range(restric_num):
                    random_number1 = random.randint(0, 1)
                    if random_number1 == 0:
                        tag_node = SubElement(logic_node, 'tag')
                        tag_node.set("info", tags[random.randint(0, M - 1)])
                    elif random_number1 == 1:
                        choose_query = random.randint(0, 2)
                        if choose_query == 0:
                            to_acc = SubElement(logic_node, 'equals')
                            to_acc.set('to', str(random.randint(0, N - 1)))
                            from_acc = SubElement(logic_node, 'equals')
                            from_acc.set('from', str(random.randint(0, N - 1)))
                        elif choose_query == 1:
                            greater_amount = SubElement(logic_node, 'greater')
                            greater_amount.set('amount', str(random.randint(0, 1000000)))
                        elif choose_query == 2:
                            less_amount = SubElement(logic_node, 'less')
                            less_amount.set('amount', str(random.randint(0, 1000000)))
            elif random_number == 1: #do tag search
                tag_node = SubElement(query_node, 'tag')
                tag_node.set("info", tags[random.randint(0, M - 1)])
            else: #do the relations
                choose_query = random.randint(0, 2)
                if choose_query == 0:
                    to_acc = SubElement(query_node, 'equals')
                    to_acc.set('to', str(random.randint(0, N - 1)))
                    from_acc = SubElement(query_node, 'equals')
                    from_acc.set('from', str(random.randint(0, N - 1)))
                elif choose_query == 1:
                    greater_amount = SubElement(query_node, 'greater')
                    greater_amount.set('amount', str(random.randint(0, 1000000)))
                elif choose_query == 2:
                    less_amount = SubElement(query_node, 'less')
                    less_amount.set('amount', str(random.randint(0, 1000000)))
    payload = convert_to_readable(top)
    try:
        # Connect to server and send data
        sock.connect((HOST, PORT))
        # send_data = str(len(payload)) + payload
        send_data = (append_zero(str(len(payload))) + payload).encode('utf-8')

        # print(send_data)
        sock.sendall(send_data)


        length = sock.recv(8)
        end_time1 = time.time()
        print("--- %s seconds ---" % (end_time1 - start_time))
        # print(length.decode("utf-8"))
        xml_length = int(length.decode("utf-8"))
        # print(xml_length)
        xml = bytes()
        while xml_length > 0:
            msg = sock.recv(4096)
            xml += msg
            xml_length -= len(msg)
        xml_string = str(xml.decode("utf-8"))
        # Receive data from the server and shut down
    finally:
        sock.close()
    # print(send_data)
    # print(xml_string)


tags = generate_tags()
create_account()
#
# for i in range(8):
#     t = threading.Thread(target=send_transfer, args=())
#     t.start()
#
# for i in range(8):
#     t = threading.Thread(target=send_balance, args=())
#     t.start()
#
# for i in range(8):
#     t = threading.Thread(target=send_balance, args=())
#     t.start()

# for i in range(1):
#     t = threading.Thread(target=send_query, args=())
#     t.start()
# start_time = time.time()
# for i in range(1000):
#     t = threading.Thread(target=send_mix, args=())
#     t.start()
# end_time = time.time()
# print("--- final: %s seconds ---" % (end_time - start_time))

start_time = time.time()
for i in range(1000):
    send_mix()
end_time = time.time()
print("--- final: %s seconds ---" % (end_time - start_time))
