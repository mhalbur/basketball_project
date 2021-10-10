def execute_sql(connection, directory, sql_file, **args):
    sql_script = pkgutil.get_data(__name__, f"{directory}/{sql_file}").decode().format(**args)
    with connection:
        data = connection.execute(str(sql_script))
    return data