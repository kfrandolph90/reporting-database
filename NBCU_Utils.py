############################################################
### NBCU Reporting Utilities
### Inserts DataFrame to Reporting Database
### Assumes table already created
### 02/20/19 Kyle Randolph 
### Email him if it breaks <3 <3 
### (kyle.randolph@essence.globalcom)
############################################################

from datetime import datetime
import pymysql
import urllib.request

## Change credentials if needed
host = '35.237.148.208'
database = 'NBCU Reporting'
username = 'drew_admin'
password = 'test'
port = 3306

connection = pymysql.connect(host=host,
                                user=username,
                                password=password,
                                db=database,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

if connection.open:
    print("Connected to {} @ {}".format(database,host))
    
# not used as of now
"""
def run_query(query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        response = cursor.fetchall()
    return response
"""

# 
def generate_tuples(df):
    """
    Converts dataframe to list of tuples and a list of column names
    """
    columns = list(df)
    tuples = [tuple(x) for x in df.values]
    return columns,tuples

# 
def generate_insert(columns,table_name):
    """
    Dynamically generates SQL INSERT query from dataframe columns list and destination table
    """
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    columns.append("transactionTime")
    column_count = len(columns)
    column_str = "`" + '`,`'.join(columns) + "`"
    column_placeholder = ",".join(["%s" for x in range(column_count-1)])
    
    q = (
        "INSERT INTO `{}` ({}) "
        "VALUES ({},\"{}\") "
        )
    
    return q.format(table_name,
                    column_str,
                    column_placeholder,
                    timestamp) # places timestamp of transaction in every row for easy rollback

def insertmany(query,values):
    """
    run insert query w/ data
    """

    if not connection.open:
        connection.connect()
        print("Connected to DB")
    with connection.cursor() as cursor:
            try:
                cursor.executemany(query,values)
                connection.commit()
                rows = cursor.rowcount
                print("Write Success!\n{} rows written".format(rows))
                connection.close()

            except Exception as e:
                print("DB Write Error\nRolling Back..")
                connection.rollback()
                connection.close()
                print(e)
                return
    
def write_to_db(df,dest_table):
    """
    main function writes dataframe to destination table
        df = pandas dataframe
        dest_table = string of destination db table
    
    Will try to write all columns of dataframe so filter before writing
    Columns need to match (case sensitive) between df and dest_table
    """
    columns,values = generate_tuples(df)
    insert_query = generate_insert(columns,dest_table)
    ##print(insert_query) uncomment for troubleshooting
    insertmany(insert_query,values)
    connection.disconnect()
    
def get_my_ip():
    external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')
    print("My External IP Address is:\n{}".format(external_ip))