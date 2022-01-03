import rich
import sqlite3


class SQLite3():
    # put databse location in encrypted config file
    def __init__(self, database_file="nba_basketball.db"):
        self.database_file = database_file
        self.sql_file = None
        self.connector = sqlite3.connect(self.database_file)
        self.insert_cnt = 0
        self.delete_cnt = 0
        self.select_cnt = 0
        self.table_cnt = 0
        self.sql_type = None

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):

        if self.sql_type == 'insert':
            rich.print(f"Number of rows inserted: {self.insert_cnt}")
        elif self.sql_type == 'delete':
            rich.print(f"Number of rows deleted: {self.delete_cnt}")
        elif self.sql_type == 'select':
            rich.print(f"Number of rows selected: {self.select_cnt}")
        elif self.sql_type == 'create':
            rich.print(f"Tables created: {self.table_cnt}")

        if isinstance(exc_value, Exception):
            self.connector.rollback()
        else:
            self.connector.commit()
        self.connector.close()

    def clean_table(self, table):
        self.sql_file = "generic/resources/delete_all.sql"
        sql = self.read_sql_file(table=table)
        self.execute_sql(sql=sql)

    def read_sql_file(self, list: list = None, **args):
        if list:
            return open(self.sql_file).read().format(*list)
        else:
            return open(self.sql_file).read().format(**args)

    def execute_sql(self, sql=None, sql_file_path=None, sql_file_name=None, **args):
        if not sql:
            self.sql_file = f'{sql_file_path}/{sql_file_name}'
            sql = self.read_sql_file(**args)
        print(sql)
        with self.connector as db:
            cnt = db.execute(sql).rowcount

        if "insert" in sql.lower():
            self.insert_cnt += cnt
            self.sql_type = 'insert'
        elif "delete" in sql.lower():
            self.delete_cnt += cnt
            self.sql_type = 'delete'
        elif "create or replace" in sql.lower():
            self.table_cnt += cnt
            self.sql_type = 'create'

    def select_sql(self, sql_file_path=None, sql_file_name=None, sql=None, **args):
        if not sql:
            self.sql_file = f'{sql_file_path}/{sql_file_name}'
            sql = self.read_sql_file(**args)

        cur = self.connector.cursor()
        cur.execute(sql)
        rows = cur.fetchall()

        self.select_cnt = len(rows)
        self.sql_type = 'select'

        return rows
