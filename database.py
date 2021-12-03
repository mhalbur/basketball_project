def read_sql_file(file_path, **args):
    query = open(file_path).read().format(**args)
    print(query)
    return(query)

def execute_sql(connection, sql, **args):
    with connection:
        data = connection.execute(sql)
    return data