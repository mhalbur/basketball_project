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
                db.execute_sql(sql_full_file_path=path)


def select_sql(sql_statements: List = None, file_paths: List = None):
    with SQLite3() as db:
        if sql_statements:
            for statement in sql_statements:
                db.select_sql(sql=statement)

        if file_paths:
            for path in file_paths:
                db.select_sql(sql_full_file_path=path)