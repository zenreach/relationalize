from datetime import datetime
from typing import Literal, NewType, TypeGuard

"""
unsupported:[type]
"""
UnsupportedColumnType = NewType('UnsupportedColumnType', str)

def is_unsupported_column_type(column: str) -> TypeGuard[UnsupportedColumnType]:
    return column.startswith('unsupported:')

"""
c-{hypen-delimited choice type list}
"""
ChoiceColumnType = NewType('ChoiceColumnType', str)

def is_choice_column_type(column: str) -> TypeGuard[ChoiceColumnType]:
    return column.startswith('c-')

BaseSupportedColumnType = Literal[
    'bool',
    'datetime',
    'float',
    'int',
    'none',
    'str',
]
SupportedColumnType = BaseSupportedColumnType | ChoiceColumnType

ColumnType = SupportedColumnType | UnsupportedColumnType

def parse_type_string(value: str):
    """
    Return data type of string if it can be parsed as a different data type
    """
    # check if bool
    value_lower = value.lower()
    if value_lower in ['true', 'false']:
        return 'bool'
    
    # check if int
    try:
        int(value)
        return 'int'
    except ValueError:
        pass

    # check if float
    try:
        float(value)
        return 'float'
    except ValueError:
        pass

    # check if datetime with format 2025-02-27 14:05:09.123456
    try:
        timestamp_format = "%Y-%m-%d %H:%M:%S.%f"
        datetime.strptime(value, timestamp_format)
        return 'datetime'
    except ValueError:
        pass

    # If not all of the above, it is just a str
    return 'str'
