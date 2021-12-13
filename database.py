import sqlite3

def read_sql_file(file_path, **args):
    query = open(file_path).read().format(**args)
    return(query)


def execute_sql(database_file="nba_basketball.db", sql_file_path=None, sql_file_name=None, sql=None, **args):
    connection = sqlite3.connect(database_file)
    
    if sql_file_path:
        file_path = f"{sql_file_path}/{sql_file_name}"
        sql = read_sql_file(file_path=file_path)

    print(sql)

    with connection:
        cnt = connection.execute(sql).rowcount

    if "insert" in sql.lower():
        print(f"{cnt} row(s) inserted...")
    elif "delete" in sql.lower():
        print(f"{cnt} row(s) deleted...")


def select_sql(database_file="nba_basketball.db", sql_file_path=None, sql_file_name=None, sql=None, **args):
    connection = sqlite3.connect(database_file)

    if sql_file_path:
        file_path = f"{sql_file_path}/{sql_file_name}"
        sql = read_sql_file(file_path=file_path)

    cur = connection.cursor()
    cur.execute(sql)

    rows = cur.fetchall()

    print(f"{len(rows)} rows selected...")
    return rows


def clean_table(table):
    sql = read_sql_file(file_path="resources/templates/delete_all.sql",  table=table)
    print(table)
    execute_sql(sql=sql)