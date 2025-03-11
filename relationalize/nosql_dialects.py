from abc import ABC, abstractmethod

from relationalize.types import SupportedColumnParam
class NoSQLDialect(ABC):
    """
    Parent class for different NoSQL dialects.
    """

    @staticmethod
    @abstractmethod
    def is_primary_key(key: str) -> bool:
        '''
        Checks if the key is a primary key
        '''
        raise NotImplementedError()


mongo_field: dict[SupportedColumnParam, str] = {
    "primary_key": "_id",
}

class MongoDialect(NoSQLDialect):
    """
    Inherits from `NoSQLDialect` and implements the mongo syntax.
    """

    @staticmethod
    def is_primary_key(key: str):
        '''
        Checks if the key is a primary key in MongoDB
        '''
        return (key == mongo_field["primary_key"])
