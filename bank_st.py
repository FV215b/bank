import socket
import psycopg2
import sys
import parse

host = ''
port = 12345

def main():
    #table initialization
    try:
        db = psycopg2.connect(dbname='bank', user='postgres', password='passw0rd') 
        print("successfully connect")
    except:
        print("Can't open database")
    cur = db.cursor()
    create_table = """
    CREATE TABLE IF NOT EXISTS
    Account(
        account_id  CHAR(64)    PRIMARY KEY,
        balance     REAL        NOT NULL
    );
    CREATE TABLE IF NOT EXISTS
    Transaction(
        id              SERIAL      PRIMARY KEY,
        to_account      CHAR(64)    NOT NULL,
        from_account    CHAR(64)    NOT NULL,
        amount          REAL        NOT NULL
    );
    CREATE TABLE IF NOT EXISTS
    Tag(
        id      SERIAL  PRIMARY KEY,
        content TEXT    UNIQUE NOT NULL
    );
    CREATE TABLE IF NOT EXISTS
    Transaction_Tag(
        transaction_id  INT     REFERENCES Transaction(id),
        tag_id          INT     REFERENCES Tag(id),
        PRIMARY KEY (transaction_id, tag_id)
    );
    """
    cur.execute(create_table)
    
    #index initialization
    create_index = """
    CREATE UNIQUE INDEX IF NOT EXISTS account_id ON Account(account_id);
    CREATE INDEX IF NOT EXISTS balance ON Account(balance);
    CREATE UNIQUE INDEX IF NOT EXISTS transaction_id ON Transaction(id);
    CREATE INDEX IF NOT EXISTS amount ON Transaction(amount);
    CREATE UNIQUE INDEX IF NOT EXISTS tag_id ON Tag(id);
    """
    cur.execute(create_index)
    db.commit()
   
    #build socket  
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except:
        print("can't create socket")
    try:
        s.bind((host, port))
    except socket.error as e:
        print(str(e))
    s.listen(5)
    while True:
        conn, addr = s.accept()
        length = conn.recv(8)
        print(length)
        xml = conn.recv(int(length.decode("utf-8")))
        print(xml)
        reply = parse.handlexml(str(xml.decode("utf-8")))
        conn.sendall(str.encode(reply))
        conn.close()

main()



