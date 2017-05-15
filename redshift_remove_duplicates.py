# @contributors Carlos Barreto & Rodrigo Vieira

import psycopg2
import getopt
import os
import sys

db_connections = {}
db = None
db_user = None
db_pwd = None
db_host = None
db_port = None

def execute_query(statement):
    print("Statement %s" % statement)
    
    conn = get_pg_conn()
    result = None
    try:
        cur = conn.cursor()
        cur.execute(statement)
        query_result = cur.fetchall()
        if query_result is not None:
            result = query_result
            print('Query Execution returned %s Results' % (len(result)))
    except Exception as e:
        print(str(e))
        
    return result
    
def get_pg_conn():
    pid = str(os.getpid())
    conn = None

    # get the database connection for this PID
    try:
        conn = db_connections[pid]
    except KeyError:
        pass
    
    if conn == None:
        try:
            options = 'keepalives=1 keepalives_idle=200 keepalives_interval=200 keepalives_count=5'
            connection_string = "host=%s port=%s dbname=%s user=%s password=%s %s" % (db_host, db_port, db, db_user, db_pwd, options)
            print(connection_string)
            conn = psycopg2.connect(connection_string)
        except Exception as e:
            print(str(e.pgerror))
            conn.close()
    
    # cache the connection
    db_connections[pid] = conn
    
    return conn
    
#return table names into a definied schema
def get_tables(schemaname):
    statement = """
        SELECT tablename FROM pg_tables WHERE schemaname='{schemaname}';
    """.format(schemaname=schemaname)
    rows = execute_query(statement)
    response = []
    for row in rows:
        response.append(row[0])
    return response
        
    
    
def get_table_metainfo(table):
    statement = """
        SELECT "column" FROM pg_table_def WHERE schemaname = 'public'
        AND tablename = '{table}'
    """.format(table=table)
    rows = execute_query(statement)
    response = []
    for row in rows:
        response.append(row[0])
    
    return response
    
def create_storage_table(table):
    ddl = """
            SELECT ddl FROM admin.v_generate_tbl_ddl 
            WHERE tablename = '{table}' AND schemaname = 'public' AND seq > 2
        """.format(table=table)
    
    table_ddl = execute_query(ddl)
    create_temp_table = """CREATE TEMP TABLE {table}_tmp """.format(table=table)
    for row in table_ddl:
        create_temp_table += " " + row[0]
    
    print("DDL to be executed")
    execute_query(create_temp_table)

def insert_into_storage(table):
    metainfo = get_table_metainfo(table)
    columns = ",".join(metainfo)
    
    copy_original_to_temp = """
        INSERT INTO {table}_tmp (
            SELECT {columns}
                FROM (
                    SELECT *,ROW_NUMBER() OVER (PARTITION BY {id} ORDER BY {id} ASC) rownum  
                FROM {table})
            WHERE rownum = 1
        )
    """.format(table=table,columns=columns,id=metainfo[0])
    
    execute_query(copy_original_to_temp)
    
def truncate_original_table(table):
    drop_table = """TRUNCATE {table}""".format(table=table)
    execute_query(drop_table)
    
def insert_into_table(table):
    rename_table = """INSERT INTO {table} (SELECT * FROM {table}_tmp)""".format(table=table)
    execute_query(rename_table)
    
def remove_duplicates(table):
    #create temp table
    print("create storage table")
    create_storage_table(table)
    
    #insert in temp table without duplicate entries
    print("insert into storage table")
    insert_into_storage(table)
    
    #truncate original table
    print("truncate original table")
    truncate_original_table(table)   
    
    #insert temp table to original table
    print("insert into original table")
    insert_into_table(table)
    
def main(argv):
    
    global db
    global db_user
    global db_pwd
    global db_host
    global db_port
    
    
    supported_args = """db= db-user= db-pwd= db-host= db-port= schema-name="""
    try:
        optlist, remaining = getopt.getopt(argv[1:], "", supported_args.split())
    except getopt.GetoptError as err:
        print(str(err))
    
    for arg, value in optlist:
        if arg == "--db":
            db = value
        if arg == "--db-user":
            db_user = value
        if arg == "--db-pwd":
            db_pwd = value
        if arg == "--db-host":
            db_host = value
        if arg == "--db-port":
            db_port = value
        if arg == "--schema-name":
            schema_name = value
            
    
    try:
        conn = get_pg_conn()
        tables = get_tables(schema_name)
        for table in tables:           
            remove_duplicates(table)
        conn.commit()
    except Exception as err:
        print(str(err))
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main(sys.argv)
