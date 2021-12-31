import sqlite3

class SQLite3():
    # put databse location in encrypted config file
    def __init__(self, database_file="nba_basketball.db"):
        self.database_file = database_file
        self.sql_file = None
        self.connector = sqlite3.connect(self.database_file)

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        if isinstance(exc_value, Exception):
            self.connector.rollback()
        else:
            self.connector.commit()
        self.connector.close()

    def clean_table(self, table):
        self.sql_file = "generic/resources/delete_all.sql"
        sql = self.read_sql_file(table=table)
        print(f'clean up the table: {table}')
        self.execute_sql(sql=sql)

    def read_sql_file(self, list: list = None, **args):
        if list:
            return open(self.sql_file).read().format(*list)
        else:
            return open(self.sql_file).read().format(**args)

    def execute_sql(self, sql=None, sql_file_path=None, sql_file_name=None, **args):
        print(sql)
        if not sql:
            self.sql_file = f'{sql_file_path}/{sql_file_name}'
            sql = self.read_sql_file(**args)

        with self.connector as db:
            cnt = db.execute(sql).rowcount

        # if "insert" in sql.lower():
        #     print(f"{cnt} row(s) inserted...")
        # elif "delete" in sql.lower():
        #     print(f"{cnt} row(s) deleted...")

    def select_sql(self, sql_file_path=None, sql_file_name=None, sql=None, **args):
        if not sql:
            self.sql_file = f'{sql_file_path}/{sql_file_name}'
            sql = self.read_sql_file(**args)

        cur = self.connector.cursor()
        cur.execute(sql)
        rows = cur.fetchall()

        print(f"{len(rows)} rows selected...")
        return rows
