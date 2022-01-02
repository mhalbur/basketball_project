from functools import wraps
from packages.connectors.sqlite import SQLite3

def coroutine(func):
    @wraps(func)
    def primer(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen
    return primer


def list_to_dict(fields: list, default="NULL"):
    fields_dict = {fields[count].lower(): default for count, i in enumerate(fields)}
    return fields_dict


def lower_dict_keys(input_dict: dict):
    output_dict = dict((k.lower(), v) for k, v in input_dict.items())
    return output_dict


def formatter(data, fields: list, none_val="NULL", empty_string_val="NULL"):
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
def loader(sql_file):
    with SQLite3() as db:
        while True:
            row = yield
            row_dict = list(row.values())
            db.sql_file = sql_file
            sql = db.read_sql_file(list=row_dict)
            db.execute_sql(sql=sql)