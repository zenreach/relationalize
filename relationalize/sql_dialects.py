from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Generic, Literal, NewType, TypeVar

from relationalize.types import SupportedColumnType, SupportedColumnParam

_COLUMN_SEPARATOR = "\n    , "

DDLColumn = NewType('DDLColumn', str)
DialectColumnType = TypeVar('DialectColumnType')

class SQLDialect(ABC, Generic[DialectColumnType]):
    """
    Parent class for different sql dialects.

    Child classes must implement the `generate_ddl_column` method
    , and provide `type_column_mapping` and `base_ddl`.
    """

    type_column_mapping: Mapping[SupportedColumnType, DialectColumnType]
    base_ddl: str

    @staticmethod
    @abstractmethod
    def generate_ddl_column(column_name: str, column_type: DialectColumnType, is_primary: bool = False) -> DDLColumn:
        raise NotImplementedError()

    def generate_ddl(self, schema: str, table_name: str, columns: list[str]):
        """
        Generates a complete "Create Table" statement given the
        schema, table_name, and column definitions.
        """
        columns_str = _COLUMN_SEPARATOR.join(columns)
        return self.base_ddl.format(
            schema=schema, table_name=table_name, columns=columns_str
        )

# PostgreSQL #

PostgresColumnType = Literal[
    'BOOLEAN',
    'INT',
    'BIGINT',
    'FLOAT',        # FLOAT === FLOAT8 === DOUBLE PRECISION
    'TEXT',
    'TIMESTAMP',    # TIMESTAMP WITHOUT TIMEZONE
    'TIMESTAMPTZ',  # TIMESTAMP WITH TIMEZONE
]

postgres_column_param: dict[SupportedColumnParam, str] = {
    "primary_key": "PRIMARY KEY",
}

class PostgresDialect(SQLDialect[PostgresColumnType]):
    """
    Inherits from `SQLDialect` and implements the postgres syntax.
    """

    type_column_mapping: Mapping[SupportedColumnType, PostgresColumnType] = {
        "none": "BOOLEAN",
        "bool": "BOOLEAN",
        "int": "INT",
        "bigint": "BIGINT",
        "float": "FLOAT",
        "str": "TEXT",
        "datetime": "TIMESTAMP",
        "datetime_tz": "TIMESTAMPTZ",
    }

    base_ddl: str = """
CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
    {columns}
);
    """.strip()

    @staticmethod
    def generate_ddl_column(column_name: str, column_type: PostgresColumnType, is_primary: bool = False):
        cleaned_column_name = column_name.replace('"', '""')
        column_str = f'"{cleaned_column_name}" {column_type}'
        if is_primary:
            column_str = f'"{cleaned_column_name}" {column_type} {postgres_column_param["primary_key"]}'
        return DDLColumn(column_str)


# Flink SQL #

FlinkColumnType = Literal[
    'BOOLEAN',
    'INT',
    'BIGINT',
    'FLOAT',
    'STRING',
    'TIMESTAMP',
    'TIMESTAMP_LTZ',
]

flink_column_param: dict[(SupportedColumnParam, str), str] = {
    "primary_key": "PRIMARY KEY",
}

class FlinkDialect(SQLDialect[FlinkColumnType]):
    """
    Inherits from `SQLDialect` and implements the Flink SQL syntax.
    """

    type_column_mapping: Mapping[SupportedColumnType, FlinkColumnType] = {
        "none": "BOOLEAN",
        "bool": "BOOLEAN",
        "int": "INT",
        "bigint": "BIGINT",
        "float": "FLOAT",
        "str": "STRING",
        "datetime": "TIMESTAMP",
        "datetime_tz": "TIMESTAMP_LTZ",
    }

    base_ddl: str = """
CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
    {columns}
);
    """.strip()

    @staticmethod
    def generate_ddl_column(column_name: str, column_type: FlinkColumnType, is_primary: bool = False):
        '''
        is_primary = True adds a primary key constraint to the column. Default is False. Since Flink does not own the data, primary keys will always be in NOT ENFORCED mode. 
        '''
        cleaned_column_name = column_name.replace('"', '""')
        column_str = f'"{cleaned_column_name}" {column_type}'
        if is_primary:
            column_str = f'"{cleaned_column_name}" {column_type} {flink_column_param["primary_key"]} NOT ENFORCED'
        return DDLColumn(column_str)
