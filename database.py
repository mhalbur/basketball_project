import sqlite3

def read_sql_file(file_path, **args):
    query = open(file_path).read().format(**args)
    # todo: set up better logging statement
    print(query)
    return(query)


def execute_sql(database_file="nba_basketball.db", sql_file_path=None, sql_file_name=None, sql=None, **args):
    connection = sqlite3.connect(database_file)
    
    if sql_file_path:
        file_path = f"{sql_file_path}/{sql_file_name}"
        sql = read_sql_file(file_path=file_path)
    
    with connection:
        data = connection.execute(str(sql))
    return data

def clean_table(table):
    sql = read_sql_file(file_path="resources/templates/delete_all.sql",  table=table)
    print(table)
    execute_sql(sql=sql)