from logging import Logger
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
            sql = db.read_sql_file(list=row_dict)
            log.info(sql)
            db.execute_sql(sql=sql)
