import re
from datetime import datetime
from typing import Literal, NewType, TypeGuard

"""
unsupported:[type]
"""
UnsupportedColumnType = NewType('UnsupportedColumnType', str)

def is_unsupported_column_type(column: str) -> TypeGuard[UnsupportedColumnType]:
    """
    Return True if the column type is unsupported
    """
    return column.startswith('unsupported:')

"""
c-{hypen-delimited choice type list}
"""
ChoiceColumnType = NewType('ChoiceColumnType', str)

def is_choice_column_type(column: str) -> TypeGuard[ChoiceColumnType]:
    return column.startswith('c-')

BaseSupportedColumnType = Literal[
    'none',
    'bool',
    'int',
    'bigint',
    'float',
    'str',
    'datetime',     # without timezone
    'datetime_tz',  # with timezone
]
SupportedColumnType = BaseSupportedColumnType | ChoiceColumnType

ColumnType = SupportedColumnType | UnsupportedColumnType

SupportedColumnParam = Literal[
    'primary_key',
]

INT_MIN = -2147483648
INT_MAX = 2147483647

def parse_type_int(value: int):
    if value < INT_MIN or value > INT_MAX:
        return 'bigint'
    return 'int'

# Initial datetime regex that matches a string starting with "%Y-%m-%d %H:%M:%S" and anything after
DATETIME_REGEX = r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}.*$'
DATETIME_VALID_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",     # With milliseconds                     (e.g. 2017-11-12 22:38:59.010000)
    "%Y-%m-%d %H:%M:%S.%f%z",   # With milliseconds, tz offset          (e.g. 2017-11-12 22:38:59.010000-0500, 2017-11-12 22:38:59.01-05:00)
    "%Y-%m-%dT%H:%M:%S.%f%z",   # With milliseconds, tz offset, T sep   (e.g. 2017-11-12T22:38:59.010000-0500, 2017-11-12T22:38:59.010000-05:00)
    "%Y-%m-%d %H:%M:%S",        # Without milliseconds                  (e.g. 2017-11-12 22:38:59)
    "%Y-%m-%dT%H:%M:%S",        # Without milliseconds, T sep           (e.g. 2017-11-12T22:38:59)
]

def parse_type_string(value: str):
    """
    Return data type of string if it can be parsed as a different data type

    Uncomment the cases relevant to you
    """
    # # check if bool
    # if value.lower() in ['true', 'false']:
    #     return 'bool'

    # # check if int
    # try:
    #     int_val = int(value)
    #     return parse_type_int(int_val)
    # except ValueError:
    #     pass

    # # check if float
    # try:
    #     float_val = float(value)
    #     if float_val.is_integer():
    #         return parse_type_int(int(float_val))
    #     return "float"
    # except ValueError:
    #     pass

    # Check if in any of the valid datetime formats 
    # First, perform a general datetime format check to limit datetime.strptime calls, improving performance
    if re.match(DATETIME_REGEX, value):
        # special case: remove the 'Z' character to handle formats with the 'Z' UTC stand-in (e.g. 2017-11-12 22:38:59.010000Z, 2017-11-12 22:38:59.011Z)
        if value.endswith('Z'):
            value = value[:-1]
        for fmt in DATETIME_VALID_FORMATS:
            try:
                datetime.strptime(value, fmt)
                return 'datetime_tz'
            except ValueError:
                continue

    # If not all of the above, leave as a str
    return 'str'
