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

SupportedColumnParam = Literal[
    'primary_key',
]

DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",      # Format with milliseconds
    "%Y-%m-%d %H:%M:%S",         # Format without milliseconds
    "%Y-%m-%d %H:%M:%S.%fZ"      # Format with milliseconds + timezone indicator
]

def parse_type_string(value: str):
    """
    Return data type of string if it can be parsed as a different data type
    """
    # # check if bool
    # value_lower = value.lower()
    # if value_lower in ['true', 'false']:
    #     return 'bool'
    
    # # check if int
    # try:
    #     int(value)
    #     return 'int'
    # except ValueError:
    #     pass

    # # check if float
    # try:
        # fval = float(value)
        # if fval.is_integer():
        #     return "int"
        # else:
        #     return "float"
    # except ValueError:
    #     pass

    # check if in valid datetime format
    for fmt in DATETIME_FORMATS:
        try:
            datetime.strptime(value, fmt)
            return 'datetime'
        except ValueError:
            continue

    # If not all of the above, leave as a str
    return 'str'
