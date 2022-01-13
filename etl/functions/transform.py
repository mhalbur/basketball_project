from typing import List, Dict


def list_to_dict(fields: List, default="NULL"):
    fields_dict = {fields[count].lower(): default for count, i in enumerate(fields)}
    return fields_dict


def lower_dict_keys(input_dict: Dict):
    output_dict = dict((str(key).lower(), value) for key, value in input_dict.items())
    return output_dict


def dict_to_list(dictionary: Dict):
    return list(dictionary.values())


def format_dict(row: Dict, fields: List, none_val="NULL", empty_string_val="NULL"):
    fields_dict = list_to_dict(fields=fields)
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
    return fields
