import xml.etree.cElementTree as ET
from xml.etree.cElementTree import Element, SubElement, XML, fromstring, tostring
from xml.dom import minidom
import psycopg2
import sys
import threading

try:
    db = psycopg2.connect(dbname='bank', user='postgres', password='passw0rd')
    print("successfully connect")
except:
    print("Can't open database")
cur = db.cursor()

lock = threading.Lock()

def handlexml(request):
    try:
        tree = ET.ElementTree(XML(request))
    except:
        top = Element('error')
        top.text = "Input cannot parse to XML"
        return convert_to_readable(top)
    root = tree.getroot()
    top = Element('results')
    if root.tag != 'transactions':
        error = SubElement(top, 'error')
        error.text = "Root name must be transactions"
        return convert_to_readable(top)
    if ('reset' in root.attrib) and (root.attrib['reset'] == 'true'):
        clean = "DELETE FROM Account;"
        try:
            cur.execute(clean)
        except:
            print("Can't clean database Account")
        clean = "DELETE FROM Transaction_Tag;"
        try:
            cur.execute(clean)
        except:
            print("Can't clean database Transaction_Tag")
        clean = "DELETE FROM Tag;"
        try:
            cur.execute(clean)
        except:
            print("Can't clean database Tag")
        clean = "DELETE FROM Transaction;"
        try:
            cur.execute(clean)
        except:
            print("Can't clean database Transaction")
        db.commit()
        print("Database cleaned")
    for child in root:
        print(child.tag, child.attrib, child.text)
        if child.tag == "create":
            handle_create(child, top)
        elif child.tag == "transfer":
            handle_transfer(child, top)
        elif child.tag == "balance":
            handle_balance(child, top)
        elif child.tag == "query":
            handle_query(child, top)
    return convert_to_readable(top)

def addxml(top, tag, node, text):
    child = SubElement(top, tag)
    if "ref" in node.attrib:
        child.set('ref', node.attrib['ref'])
    child.text = text

def convert_to_readable(element):
    string = tostring(element, 'utf-8')
    parsed = minidom.parseString(string)
    return parsed.toprettyxml(indent="    ")

def handle_create(create_node, top):# add reponse parameter to get the responding response
    accounts = create_node.findall('account')
    if len(accounts) != 1:
        addxml(top, 'error', create_node, 'Exactly one account can be created')
        return
    print(accounts[0].text) #check whether we can create the account
    if not is_valid_64_bit(accounts[0].text):
        addxml(top, 'error', create_node, 'Account format error')
        return

    account_num = to_64_char(accounts[0].text)
    balances = create_node.findall('balance')
    if len(balances) > 1:
        addxml(top, 'error', create_node, 'Too many input balances')
        return
    if len(balances) == 0:
        if create_account(account_num, 0):
            addxml(top, 'success', create_node, 'created')
        else:
            addxml(top, 'error', create_node, "Can't create account with default balance")
        return

    if is_valid_float_number(balances[0].text):
        if create_account(account_num, float(balances[0].text)):
            addxml(top, 'success', create_node, 'created')
        else:
            addxml(top, 'error', create_node, "Can't create account with user-setting balance")
    else:
        addxml(top, 'error', create_node, 'Balance has format error')

def to_64_char(str):
    prefix = '0' * (20 - len(str))
    return prefix + str

def is_valid_64_bit(account_num):
    try:
        return int(account_num) < 18446744073709551616 and int(account_num) >= 0
    except ValueError:
        return False

def is_valid_float_number(value):
    if value == "NaN":
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False

def create_account(account_num, money): #need to check the db/data structure
    exist = "INSERT INTO Account (account_id, balance) VALUES (%s, %s) ON CONFLICT (account_id) DO NOTHING RETURNING account_id;"
    data = (account_num, money)
    print(exist)
    try:
        cur.execute(exist, data)
    except:
        print("Can't execute. Insert failed")
        return False
    try:
        print(cur.fetchone()[0])
        db.commit()
        print("Create account successfully")
        return True
    except:
        print("Account existed. Insert failed")
        return False

def is_valid_account(account_num): #check 64 bit unsigned number and check in db/data structure
    if not is_valid_64_bit(account_num):
        return False
    exist = "SELECT * FROM Account WHERE account_id = '" + to_64_char(account_num) + "';"
    print(exist)
    try:
        cur.execute(exist)
    except:
        print("Can't execute is_valid_account")
        return False
    if cur.fetchone() is not None:
        print("Account exist")
        return True
    else:
        print("Account not exist")
        return False

def has_enough_money(account_num, money):
    #check whether the account has enough money to pay
    balance = "SELECT balance FROM Account WHERE account_id = '" + account_num + "';"
    print(balance)
    try:
        cur.execute(balance)
    except:
        print("Can't get balance")
    print("account balance: ")
    return cur.fetchone()[0] >= money

def update_from_account_balance(account_num, amount):
    update_from_account_balance = "UPDATE Account SET balance=balance-" + str(amount) + " WHERE account_id='" + account_num + "' AND balance>=" + str(amount) + " RETURNING account_id;"
    print(update_from_account_balance)
    try:
        cur.execute(update_from_account_balance)
    except:
        print("Can't execute update_from_account_balance")
        return False
    if cur.fetchone() is not None:
        print("Successfully updated source account")
        return True
    else:
        print("Update source account failed")
        return False

def update_to_account_balance(account_num, amount):
    update_to_account_balance = "UPDATE Account SET balance=balance+" + str(amount) + " WHERE account_id='" + account_num + "' RETURNING account_id;"
    print(update_to_account_balance)
    try:
        cur.execute(update_to_account_balance)
    except:
        print("Can't execute update_to_account_balance")
        return False
    if cur.fetchone() is not None:
        print("Successfully updated target account")
        return True
    else:
        print("Update target account failed")
        return False

def handle_transfer(transfer_node, top):
    tos = transfer_node.findall('to')
    if len(tos) != 1:
        addxml(top, 'error', transfer_node, 'Exactly one target account is allowed')
        return
    froms = transfer_node.findall('from')
    if len(froms) != 1:
        addxml(top, 'error', transfer_node, 'Exactly one source account is allowed')
        return
    amounts = transfer_node.findall('amount')
    if len(amounts) != 1:
        addxml(top, 'error', transfer_node, 'Exactly one transfer amount is allowed')
        return
    #check the to and from exist
    if not is_valid_64_bit(tos[0].text):
        addxml(top, 'error', transfer_node, 'Target account format error')
        return
    if not is_valid_64_bit(froms[0].text):
        addxml(top, 'error', transfer_node, 'Source account format error')
        return
    if not is_valid_float_number(amounts[0].text):
        addxml(top, 'error', transfer_node, 'Transfer amount has format error')
        return
    to_account = to_64_char(tos[0].text)
    from_account = to_64_char(froms[0].text)
    amount = float(amounts[0].text)
    
    lock.acquire()
    if is_valid_account(tos[0].text):
        if update_from_account_balance(from_account, amount):
            if update_to_account_balance(to_account, amount):
                db.commit()
                lock.release()
                addxml(top, 'success', transfer_node, 'transferred')
                print("Successfully update accounts")
            else:
                addxml(top, 'error', transfer_node, 'Target account update error')
                lock.release()
                return
        else:
            addxml(top, 'error', transfer_node, 'Source account not exist or balance is not enough')
            lock.release()
            return
    else:
        addxml(top, 'error', transfer_node, 'Target account not exist')
        lock.release()
        return
    

    #insert tags
    tags_values = []
    tag_ids = []
    tags = transfer_node.findall('tag')
    for tag in tags:
        tags_values.append("('" + tag.text + "')")
    tags_values_to_string = ','.join(tags_values)
    if not tags_values_to_string:
        print("Tag_values_to_string is empty")
    else:
        insert_tags_sql = "INSERT INTO Tag (content) VALUES " + tags_values_to_string + " ON CONFLICT (content) DO NOTHING;"
        print(insert_tags_sql)
        try:
            cur.execute(insert_tags_sql)
            db.commit()
            print("Insert tags Done!")
        except:
            print("Can't insert tags")
        tags_query = []
        for tag in tags:
            tags_query.append("'" + tag.text + "'")
        tags_query_to_string = ','.join(tags_query)
        select_tags_sql = "SELECT id FROM Tag WHERE content IN (" + tags_query_to_string + ");"
        print(select_tags_sql)
        try:
            cur.execute(select_tags_sql)
            tag_ids = cur.fetchall()
            print(tag_ids)
            print("Query tags Done!")
        except:
            print("Can't query tags")
    
    #insert transaction
    tags_values = []
    tags = transfer_node.findall('tag')
    for tag in tags:
        tags_values.append(tag.text)
    insert_transaction_sql = "INSERT INTO Transaction (to_account, from_account, amount, tags) VALUES (%s, %s, %s, %s) RETURNING id;"
    data = (to_account, from_account, amount, tags_values)
    try:
        cur.execute(insert_transaction_sql, data)
        transaction_id = cur.fetchone()[0]  
        print(transaction_id)
        db.commit()
        print("Insert transaction done!")
    except:
        print("Can't insert transaction")

    #link transaction to tags
    transaction_tag_pairs = []
    for tag_id in tag_ids:
        transaction_tag_pairs.append("(" + str(transaction_id) + "," + str(tag_id[0]) + ")")
    transaction_tag_pairs_to_string = ','.join(transaction_tag_pairs)
    if transaction_tag_pairs_to_string:
        insert_transaction_tag_sql = "INSERT INTO Transaction_Tag (transaction_id, tag_id) VALUES " + transaction_tag_pairs_to_string + ";"
        print(insert_transaction_tag_sql)
        try:
            cur.execute(insert_transaction_tag_sql)
            db.commit()
            print("insert_transaction_tag done!")
        except:
            print("Can't insert_transaction_tag")

def handle_balance(balance_node, top):
    accounts = balance_node.findall('account')
    if len(accounts) != 1:
        addxml(top, 'error', balance_node, 'Exactly one account is allowed')
        return
    if not is_valid_account(accounts[0].text):
        addxml(top, 'error', balance_node, 'Account does not exist')
        return
    addxml(top, 'success', balance_node, check_balance(to_64_char(accounts[0].text)))

def check_balance(account_num): #need to check db/data structure to query
    balance_query = "SELECT balance FROM Account WHERE account_id = '" + account_num + "';"
    print(balance_query)
    try:
        cur.execute(balance_query)
    except:
        print("Can't get balance")
    return str(cur.fetchone()[0])

def clean_append_zero(str):
    for i in range(20):
        if str[i] != '0':
            return str[i:]
        if i == 19:
            return '0'

def addqueryxml(final_query, results):
    query_sentence = "SELECT from_account, to_account, amount, tags FROM Transaction"
    if final_query != "":
        query_sentence += " WHERE " + final_query
    query_sentence += ";"
    print(query_sentence)
    cur.execute(query_sentence)
    query_result = cur.fetchall()
    for q in query_result:
        transfer = SubElement(results, 'transfer')
        from_tag = SubElement(transfer, 'from')
        from_tag.text = clean_append_zero(q[0].replace(" ",""))
        print("from: " + from_tag.text)
        to_tag = SubElement(transfer, 'to')
        to_tag.text = clean_append_zero(q[1].replace(" ",""))
        print("to: " + to_tag.text)
        amount_tag = SubElement(transfer, 'amount')
        amount_tag.text = str(q[2])
        print("amount: " + amount_tag.text)
        tags = SubElement(transfer, 'tags')
        for t in q[3]:
            tag = SubElement(tags, 'tag')
            tag.text = t
            print("tag: " + tag.text)

def handle_query(query_node, top):
    flag_container = []
    flag_container.append(False)
    final_query = dfs(query_node, flag_container)
    if flag_container[0]:
        results = addxml(top, 'error', query_node, 'Query format error')
    else:
        results = SubElement(top, 'results')
        if "ref" in query_node.attrib:
            results.set('ref', query_node.attrib['ref'])
        print(final_query)
        addqueryxml(final_query, results)

def is_valid_relation(child):
    attr_num = 0
    if "to" in child.attrib:
        attr_num += 1
    if "from" in child.attrib:
        attr_num += 1
    if "amount" in child.attrib:
        attr_num += 1
    return (attr_num == 1)

def is_valid_tag(child):
    return ("info" in child.attrib)

def dfs(root, flag_container):
    if flag_container[0]:
        return ""
    if root.tag == "equals":
        if is_valid_relation(root):
            if "from" in root.attrib:
                if is_valid_64_bit(root.attrib['from']):
                    return "from_account='" + to_64_char(root.attrib['from']) + "'"
                else:
                    flag_container[0] = True
                    return ""
            elif "to" in root.attrib:
                if is_valid_64_bit(root.attrib['to']):
                    return "to_account='" + to_64_char(root.attrib['to']) + "'"
                else:
                    flag_container[0] = True
                    return ""
            elif "amount" in root.attrib:
                if is_valid_float_number(root.attrib['amount']):
                    return "amount=" + root.attrib['amount']
                else:
                    flag_container[0] = True
                    return ""
        else:
            flag_container[0] = True
            return ""
    elif root.tag == "less":
        if is_valid_relation(root):
            if "from" in root.attrib:
                if is_valid_64_bit(root.attrib['from']):
                    return "from_account<'" + to_64_char(root.attrib['from']) + "'"
                else:
                    flag_container[0] = True
                    return ""
            elif "to" in root.attrib:
                if is_valid_64_bit(root.attrib['to']):
                    return "to_account<'" + to_64_char(root.attrib['to']) + "'"
                else:
                    flag_container[0] = True
                    return ""
            elif "amount" in root.attrib:
                if is_valid_float_number(root.attrib['amount']):
                    return "amount<" + root.attrib['amount']
                else:
                    flag_container[0] = True
                    return ""
        else:
            flag_container[0] = True
            return ""
    elif root.tag == "greater":
        if is_valid_relation(root):
            if "from" in root.attrib:
                if is_valid_64_bit(root.attrib['from']):
                    return "from_account>'" + to_64_char(root.attrib['from']) + "'"
                else:
                    flag_container[0] = True
                    return ""
            elif "to" in root.attrib:
                if is_valid_64_bit(root.attrib['to']):
                    return "to_account>'" + to_64_char(root.attrib['to']) + "'"
                else:
                    flag_container[0] = True
                    return ""
            elif "amount" in root.attrib:
                if is_valid_float_number(root.attrib['amount']):
                    return "amount>" + root.attrib['amount']
                else:
                    flag_container[0] = True
                    return ""
        else:
            flag_container[0] = True
            return ""
    elif root.tag == "tag":
        if is_valid_tag(root):
            tag_query = "SELECT id FROM Tag WHERE content='" + root.attrib['info'] + "';"
            cur.execute(tag_query)
            tag_id = cur.fetchone()
            if tag_id is not None:
                transaction_query = "SELECT transaction_id FROM Transaction_Tag WHERE tag_id=" + str(tag_id[0]) + ";"
                cur.execute(transaction_query)
                transaction_ids = cur.fetchall()
                trans_list = []
                for trans in transaction_ids:
                    trans_list.append("id=" + str(trans[0]))
                return "(" + ' OR '.join(trans_list) + ")"
            else:
                return ""
        else:
            flag_container[0] = True
            return ""
    elif (root.tag == "or") or (root.tag == "and") or (root.tag == "not") or (root.tag == "query"):
        children_list = []
        for child in root:
            dfs_value = dfs(child, flag_container)
            if dfs_value != "":
                children_list.append(dfs_value)
        if (root.tag == "and") or (root.tag == "query"):
            if not children_list:
                return ""
            else:
                return "(" + ' AND '.join(children_list) + ")"
        elif (root.tag == "or"):
            if not children_list:
                return ""
            else:
                return "(" + ' OR '.join(children_list) + ")"
        else:
            if not children_list:
                return ""
            else:
                return "(NOT (" + ' AND '.join(children_list) + "))"
    else:
        return ""

