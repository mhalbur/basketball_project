from functools import wraps


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


def formatter(data, fields: list):
    fields_dict = list_to_dict(fields=fields)
    for row in data:
        lower_row = lower_dict_keys(input_dict=row)
        fields = {}
        for field in fields_dict:
            try:
                fields[field] = lower_row[field]
            except KeyError:
                continue
        yield fields
