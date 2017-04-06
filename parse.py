import xml.etree.cElementTree as ET
from xml.etree.cElementTree import Element, SubElement, XML, fromstring, tostring
from xml.dom import minidom
import psycopg2
import sys

try:
    db = psycopg2.connect(dbname='bank', user='postgres', password='passw0rd') 
    print("successfully connect")
except:
    print("Can't open database")
cur = db.cursor()

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
        clean = "DELETE FROM Transaction;"
        try:
            cur.execute(clean)
        except:
            print("Can't clean database Transaction")
        clean = "DELETE FROM Tag;"
        try:
            cur.execute(clean)
        except:
            print("Can't clean database Tag")
        clean = "DELETE FROM Transaction_Tag;"
        try:
            cur.execute(clean)
        except:
            print("Can't clean database Transaction_Tag")
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
    if not can_create_account(accounts[0].text):
        addxml(top, 'error', create_node, 'Account cannot be created')
        return

    account_num = to_64_char(accounts[0].text)
    balances = create_node.findall('balance')
    if len(balances) > 1:
        addxml(top, 'error', create_node, 'Too many input balances')
        return
    if len(balances) == 0:
        addxml(top, 'success', create_node, 'created')
        insert = """
            INSERT INTO Account (account_id, balance) VALUES (%s, %s);
        """
        data = (account_num, 0)
        try:
            cur.execute(insert, data)
        except:
            print("Can't create account")
        db.commit()
        return

    if is_valid_float_number(balances[0].text):
        addxml(top, 'success', create_node, 'created')
        insert = """
            INSERT INTO Account (account_id, balance) VALUES (%s, %s);
        """
        data = (account_num, float(balances[0].text))
        try:
            cur.execute(insert, data)
        except:
            print("Can't create account")
        db.commit()
    else:
        addxml(top, 'error', create_node, 'Balance has format error')

def to_64_char(str):
    prefix = '0' * (20 - len(str))
    return prefix + str

def is_valid_float_number(value):
    if value == "NaN":
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False

def can_create_account(account_num): #need to check the db/data structure
    #check 64 bit unsigned number
    if is_valid_64_bit(account_num):
        
        exist = "SELECT EXISTS (SELECT 1 FROM Account WHERE account_id = '" + to_64_char(account_num) + "');" 
        try:
            cur.execute(exist)
        except:
            print("Can't execute")
        print(cur.fetchone()[0])
        if cur.fetchone() is not None:
            return False
    else:
        return False
    return True

def is_valid_64_bit(account_num):
    try:
        return int(account_num) < 18446744073709551616 and int(account_num) >= 0
    except ValueError:
        return False

def is_valid_account(account_num):
    #check 64 bit unsigned number and check in db/data structure
    if not is_valid_64_bit(account_num):
        return False
    return Account.objects.filter(account_id=to_64_char(account_num)).exists()

def has_enough_money(account_num, money):
    #check whether the account has enough money to pay
    account = Account.objects.get(account_id=account_num)
    return account.balance >= money

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
    if not is_valid_account(tos[0].text):
        addxml(top, 'error', transfer_node, 'Target account does not exist')
        return
    if not is_valid_account(froms[0].text):
        addxml(top, 'error', transfer_node, 'Source account does not exist')
        return
    if not is_valid_float_number(amounts[0].text):
        addxml(top, 'error', transfer_node, 'Transfer amount has format error')
        return
    to_account = to_64_char(tos[0].text)
    from_account = to_64_char(froms[0].text)
    amount = float(amounts[0].text)
    if not has_enough_money(from_account, amount):
        addxml(top, 'error', transfer_node, 'Source account does not have enough money')
        return
        transaction = Transaction()
    transaction.to_account = Account.objects.get(account_id=to_account)
    transaction.from_account = Account.objects.get(account_id=from_account)
    transaction.amount = amount
    tags = transfer_node.findall('tag')
    transaction.save()
    for tag in tags:
        if Tag.objects.filter(content=tag.text).exists():
            transaction.tags.add(Tag.objects.get(content=tag.text))
        else:
            new_tag = Tag()
            new_tag.content = tag.text
            new_tag.save()
            transaction.tags.add(new_tag)
    addxml(top, 'success', transfer_node, 'transferred')

def handle_balance(balance_node, top):
    accounts = balance_node.findall('account')
    if len(accounts) != 1:
        addxml(top, 'error', balance_node, 'Exactly one account is allowed')
        return
    if not is_valid_account(accounts[0].text):
        addxml(top, 'error', balance_node, 'Account does not exist')
        return
    addxml(top, 'success', balance_node, check_balance(to_64_char(accounts[0].text)))

def check_balance(account): #need to check db/data structure to query
    acc = Account.objects.get(account_id=account)
    return str(acc.balance)

def clean_append_zero(str):
    for i in range(20):
        if str[i] != '0':
            return str[i:]
        if i == 19:
            return '0'

