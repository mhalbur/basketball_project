import sqlite3

def read_sql_file(file_path, **args):
    query = open(file_path).read().format(**args)
    # todo: set up better logging statement
    print(query)
    return(query)


def execute_sql(database_file, sql, **args):
    connection = sqlite3.connect(database_file)
    with connection:
        data = connection.execute(str(sql))
    return data