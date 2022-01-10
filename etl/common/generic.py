import csv
import logging
from typing import Generator, List

from etl.common.transform import coroutine, list_to_dict, lower_dict_keys
from etl.connectors.sqlite import SQLite3

log = logging.getLogger(__name__)

def formatter(data: Generator, fields: List, none_val="NULL", empty_string_val="NULL"):
    fields_dict = list_to_dict(fields=fields)
    for row in data:
        lower_row = lower_dict_keys(input_dict=row)
        fields = {}
        for field in fields_dict:
            try:
                if fields_dict[field] is None:
                    fields[field] = none_val
                elif fields_dict[field] == '':
                    fields[field] = empty_string_val
                else:
                    fields[field] = lower_row[field]
            except KeyError:
                continue
        yield fields


@coroutine
def loader(sql_file: str):
    with SQLite3() as db:
        while True:
            row = yield
            row_dict = list(row.values())
            db.sql_file = sql_file
            sql = db.read_sql_file(read_list=row_dict)
            log.info(sql)
            db.execute_sql(sql=sql)


@coroutine
def file_put_rows(file_path: str, encoding: str = 'utf-8'):
    file = open(file_path, 'w', encoding=encoding)
    writer = csv.writer(file, quoting=csv.QUOTE_ALL)
    try:
        while True:
            row = yield
            writer.writerow(row)
    except GeneratorExit as ex:
        raise ex
    finally:
        file.close()


def file_get_rows(file_path: str, file_name: str, delimiter: str = ','):
    file_path = f'{file_path}/{file_name}'

    with open(file_path) as f:
        csv_reader = csv.reader(f, delimiter=delimiter)
        for row in csv_reader:
            yield row