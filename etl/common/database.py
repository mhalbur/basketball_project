from etl.connectors.sqlite import SQLite3


def clean_table(tables: list):
    with SQLite3() as db:
        for table in tables:
            db.clean_table(table=table)


def execute_sql(sql_statements: list = None, file_paths: list = None):
    with SQLite3() as db:
        if sql_statements:
            for statement in sql_statements:
                db.execute_sql(sql=statement)

        if file_paths:
            for path in file_paths:
                db.execute_sql(sql_full_file_path=path)
