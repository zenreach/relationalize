from collections.abc import Iterable
import copy
import json
import logging
from types import TracebackType
from typing import Any, Callable, TextIO
from uuid import uuid4

from .utils import no_op, create_local_file

_DELIMITER = "_"
_ID_PREFIX = "R"
_ID = f"{_DELIMITER}rid{_DELIMITER}"
_VAL = f"{_DELIMITER}val{_DELIMITER}"
_INDEX = f"{_DELIMITER}index{_DELIMITER}"

DEFAULT_LOCAL_FILE_CALLABLE = create_local_file()
DEFAULT_LOGLEVEL = logging.WARNING

class Relationalize:
    """
    A class/utility for relationalizing JSON content.

    stringify_arrays = False by default, causing array fields to be separated into individual tables. Set stringify_arrays = True to convert arrays to string.
    stringify_objects = False by default, causing nested object fields to be flattened. Set stringify_objects = True to convert nested objects to string.
    ```
    with Relationalize('abc') as r:
        r.relationalize([{"a": 1}])
    ```
    """

    def __init__(
        self,
        name: str,
        create_output: Callable[[str], TextIO] = DEFAULT_LOCAL_FILE_CALLABLE,
        on_object_write: Callable[[str, dict[str, Any]], None] = no_op,
        stringify_arrays: bool = False,
        stringify_objects: bool = False,
        log_level=DEFAULT_LOGLEVEL
    ):
        self.name = name
        self.create_output = create_output
        self.on_object_write = on_object_write
        self.stringify_arrays = stringify_arrays
        self.stringify_objects = stringify_objects
        self.outputs: dict[str, TextIO] = {}

        # Configure logger
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(log_level)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - [%(name)s, %(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        self.logger = logger

    def __enter__(self):
        return self

    def __exit__(
        self,
        _type: type[BaseException] | None,
        _value: BaseException | None,
        _traceback: TracebackType | None
    ) -> None:
        self.close_io()

    def relationalize(self, object_list: Iterable[dict[str, object]]):
        """
        Main entrypoint into this class.

        Pass in an Iterable and it will relationalize it, outputing to wherever was designated when instantiating the class.
        """
        for item in object_list:
            self._write_to_output(self.name, self._relationalize(item))

    def _write_row(self, key: str, row: dict[str, Any]):
        """
        Writes a row to the given output.
        """
        _ = self.outputs[key].write(json.dumps(row))
        _ = self.outputs[key].write("\n")
        self.on_object_write(key, row)

    def _write_to_output(
        self, key: str, content: dict[str, Any] | list[dict[str, Any]], is_sub: bool = False
    ):
        """
        Writes content, either a single object, or a list of objects to the output.

        Will create a new TextIO if needed.
        """
        identifier = f"{self.name}{_DELIMITER}{key}" if is_sub else key
        if identifier not in self.outputs:
            self.outputs[identifier] = self.create_output(identifier)
        if isinstance(content, list):
            for row in content:
                self._write_row(identifier, row)
            return
        self._write_row(identifier, content)

    def _list_helper(self, id: str, index: int, row: dict[str, object] | Any, path: str):
        """
        Helper for relationalizing lists.

        Handles the difference between an array of literals and an array of structs.
        """
        new_row: dict[str, object] = copy.deepcopy(row)
        if isinstance(row, dict):
            new_row[_ID] = id
            new_row[_INDEX] = index
            return self._relationalize(new_row, path=path, from_array=True, table_path=path)

        return self._relationalize({_VAL: new_row, _ID: id, _INDEX: index}, path=path, from_array=True, table_path=path)

    def _relationalize(self, d: list[Any] | dict[str, Any] | str, path: str = "", from_array: bool = False, table_path: str = ""):
        """
        Recursive back bone of the relationalize structure.

        Traverses any arbitrary JSON structure flattening and relationalizing.

        from_array = True indicates that we are relationalizing a field that is sourced from an array.
        This means that subsequent column names will not be not prefixed with path_prefix, but any newly created subtables will retain their path history.
        """
        path_prefix = f"{path}{_DELIMITER}"
        if path == "" or from_array:
            path_prefix = ""
        if isinstance(d, list):
            if len(d) == 0:
                return {path: None}
            
            if self.stringify_arrays:
                d_str = str(d)
                return {path: d_str}

            id = Relationalize._generate_rid()
            for index, row in enumerate(d):
                key_path = path
                if table_path:
                    key_path = table_path
                self._write_to_output(
                    key=f"{key_path}", content=self._list_helper(id, index, row, path=path), is_sub=True
                )
            return {path: id}
            
        if isinstance(d, dict):
            if path != "" and self.stringify_objects:
                d_str = str(d)
                return {path: d_str}

            temp_d: dict[str, object] = {}
            for key in d:
                temp_table_path = ""
                if from_array:
                    temp_table_path = f"{table_path}{_DELIMITER}{key}"
                temp_d.update(self._relationalize(d[key], path=f"{path_prefix}{key}", table_path=temp_table_path))
            return temp_d

        return {path: d}

    def close_io(self) -> None:
        for file_object in self.outputs.values():
            file_object.close()

    @staticmethod
    def _generate_rid() -> str:
        """
        Generates a relationalize ID. EX:`R_2d0418f3b5de415086f1297cf0a9d9a5`
        """
        return f"{_ID_PREFIX}{_DELIMITER}{uuid4().hex}"
