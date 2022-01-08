from functools import wraps
from typing import List, Dict, Generator


def coroutine(func):
    @wraps(func)
    def primer(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen
    return primer


def list_to_dict(fields: List, default="NULL"):
    fields_dict = {fields[count].lower(): default for count, i in enumerate(fields)}
    return fields_dict


def lower_dict_keys(input_dict: Dict):
    output_dict = dict((k.lower(), v) for k, v in input_dict.items())
    return output_dict


def dict_to_list(dictionary: Dict):
    return list(dictionary.values())


@coroutine
def generator_dict_to_list(data: Generator):
    for row in data:
        yield list(row.values())