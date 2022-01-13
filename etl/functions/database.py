from typing import List
from etl.connectors.sqlite import SQLite3


def clean_table(tables: List):
    with SQLite3() as db:
        for table in tables:
            db.clean_table(table=table)


def execute_sql(sql_statements: List = None, file_paths: List = None):
    with SQLite3() as db:
        if sql_statements:
            for statement in sql_statements:
                db.execute_sql(sql=statement)

        if file_paths:
            for path in file_paths:
                db.execute_sql(file_path=path)


def select_sql(sql_statement: str = None, file_path: str = None):
    with SQLite3() as db:
        if sql_statement:
            rtn = db.select_sql(sql=sql_statement)
        else:
            rtn = db.select_sql(file_path=file_path)

        return rtn


def load_sql(file_paths: List, clean_table: bool = True, sql_file_path: str = None, sql: str = None):
    with SQLite3() as db:
        if sql:
            for file in file_paths:
                db.load_sql(load_file_path=file, sql=sql, clean_table=clean_table)

        if sql_file_path:
            for file in file_paths:
                db.load_sql(load_file_path=file, file_path=sql_file_path, clean_table=clean_table)
