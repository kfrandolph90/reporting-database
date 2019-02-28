import json
import re
import pymysql

def load_sql():
    with open('build_db.sql', encoding='utf-8') as f:
            queries = f.read()    
    queries = f.split(";")
    return queries


def build_db():
    print("Let's Build Your Reporting Database\nWhere should I connect to?")
    host = input("Host: ")
    user = input("User: ")
    pwd = input("Password: ")
    #port = input("Port (default 3306): ")

    choice = input("1. Create DB and Tables?\n2. Just Create Tables (Database Exists)?\nChoice: ")
    print(choice,type(choice))
    print("Attempting to connect to host")
    conn = pymysql.connect(host=host,
                                user=user,
                                password=pwd,
                                port=3306, 
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

    print("Connected to host..")

    with open('build_db.sql', encoding='utf-8') as f:
                q = f.read()
            
    queries = q.split(";") #split sql file on query delimiter

    if choice == "1":
        db_name = input("Desired Datebase Name: ")        
        try:
            conn.cursor().execute("CREATE DATABASE {}".format(db_name))
            conn.commit()
            print("DATABASE {} Created".format(db_name))
        
        except Exception as e:
            conn.rollback()
            print("Table Creation Error")
            print(e)
    
    elif choice == "2":
        db_name = input("Current Database Name: ")
    
    elif choice not in ["1","2"]:
        print("Invalid Choice. Run script again")
        exit()

    # switch to newly created db or existing db specified
    conn.select_db(db_name)
    
    for query in queries[0:-1]: # shitty hack to not suck in "" at the end of the query split
        table = re.search(r'CREATE [A-Z]+ `([a-zA-Z_]+)`',query).group(1)
        print("Creating {} Table".format(table))
        
        try:
            conn.cursor().execute(query)
            conn.commit()
            print("Table Created")
        except Exception as e:
            conn.rollback()
            print("Table Creation Error")
            print(e)

    print("Donezo")
    
    if conn.open:
        conn.close()
        print("Closing Connection. Have a groovy day")

if __name__ == "__main__":
    build_db()
    
