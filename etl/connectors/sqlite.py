import csv
import gzip
import logging
import sqlite3
from sqlite3 import Error

from sql_metadata import Parser


class SQLite3():
    # put databse location in encrypted config file
    def __init__(self, database_file="nba_basketball.db"):
        self.log = logging.getLogger(__name__)
        self.database_file = database_file
        self.connector = None
        self.cur = None
        self.file = None

        # logging
        self.table = None
        self.sql_types = []
        self.insert_cnt = 0
        self.delete_cnt = 0
        self.select_cnt = 0
        self.table_cnt = 0

    def __enter__(self):
        self.log = logging.getLogger(__name__)
        self.create_connection()
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        if 'insert' in self.sql_types:
            self.log.info(f"Number of rows inserted into {self.table}: {self.insert_cnt}")

        if 'delete' in self.sql_types:
            self.log.info(f"Number of rows deleted from {self.table}: {self.delete_cnt}")

        if 'select' in self.sql_types:
            self.log.info(f"Number of rows selected from {self.table}: {self.select_cnt}")

        if 'create' in self.sql_types:
            self.log.info(f"Tables created: {self.table_cnt}")

        if self.file:
            self.file.close()

        if isinstance(exc_value, Exception):
            self.connector.rollback()
        else:
            self.connector.commit()
        self.connector.close()

    def create_connection(self):
        """Establish connection for SQLite File Database
        """
        try:
            self.connector = sqlite3.connect(self.database_file)
            self.cur = self.connector.cursor()
            self.log.info("Connection was successful")
        except Error as e:
            self.log.info(f"The error '{e}' occurred.")

    def clean_table(self, table: str):
        """Empties table when called

        Args:
            table (str): table name to be emptied
        """
        file = "etl/sql_files/delete_all.sql"
        self.execute_sql(file_path=file, table=table)

    def read_sql_file(self, file_path: str, read_list: list = None, **args):
        """Opens, reads, and formats arguments for a given SQL file

        Args:
            file_path (str): file path of sql file to be read
            read_list (list, optional): List of arguments that would be passed into file to be formatted. Defaults to None.

        Returns:
            [str]: A string containing the formatted SQL query to be ran in a later step
        """
        if read_list:
            return open(file_path).read().format(*read_list)
        else:
            return open(file_path).read().format(**args)

    def sql_logging(self, sql: str, cnt: int):
        """Keeps track of counts and sql query types for logging when connection is closed

        Args:
            sql (str): SQL query that was ran before being passed to sql_logging()
            cnt (int): Count of rows selected, inserted, deleted or tables created
        """
        if "insert" in sql.lower():
            self.insert_cnt += cnt
            self.sql_types.append('insert')

        if "delete" in sql.lower():
            self.delete_cnt += cnt
            self.sql_types.append('delete')

        if "create or replace" in sql.lower():
            self.table_cnt += cnt
            self.sql_types.append('create')

        if "select" in sql.lower() and "insert" not in sql.lower() and "delete" not in sql.lower():
            self.select_cnt += cnt
            self.sql_types.append('select')

    def execute_sql(self, sql: str = None, file_path: str = None, **args):
        """Executes a SQL query from either SQL str or a file_path

        Args:
            sql (str, optional): SQL query to be ran. Defaults to None.
            file_path (str, optional): File path to the location of the file containing the SQL to be ran. Defaults to None.
        """
        if file_path:
            sql = self.read_sql_file(file_path=file_path, **args)

        cnt = self.cur.execute(sql).rowcount

        # ValueError: Not supported query type
        if 'delete' not in sql:
            self.table = Parser(sql).tables[0]

        self.sql_logging(sql=sql, cnt=cnt)

    def select_sql(self, sql: str = None, file_path: str = None, **args):
        """Executes a select SQL query and returns its results

        Args:
            sql (str, optional): SQL query to be ran. Defaults to None.
            file_path (str, optional): File path to the location of the file containing the SQL to be ran. Defaults to None.

        Returns:
            [List]: All rows of a query result that is retrieved from the ran query
        """
        if file_path:
            sql = self.read_sql_file(file_path=file_path, **args)

        self.cur.execute(sql)
        rows = self.cur.fetchall()

        self.sql_logging(sql=sql, cnt=len(rows))
        self.table = Parser(sql).tables[0]

        return rows

    def load_sql(self, load_file_path: str, clean_table: bool = True,  sql: str = None, file_path: str = None):
        """Reads csv files and loads it into a database

        Args:
            load_file_path (str): location of the file to be loaded
            clean_table (bool, optional): If True, the table will be emptied before loading, otherwise, if False, it will not. Defaults to True.
            sql (str, optional): SQL query to be ran. Defaults to None.
            file_path (str, optional): File path to the location of the file containing the SQL to be ran. Defaults to None.
        """
        if file_path:
            sql = self.read_sql_file(file_path=file_path)

        self.table = Parser(sql).tables[0]

        if clean_table:
            self.clean_table(table=self.table)

        print(load_file_path)

        file_extension = load_file_path.split('.')[-1]

        if file_extension == "gz":
            self.file = gzip.open(load_file_path, 'rt')
        else:
            self.file = open(load_file_path, 'r')

        rows = csv.reader(self.file)
        cnt = self.cur.executemany(sql, rows).rowcount

        self.sql_logging(sql=sql, cnt=cnt)
