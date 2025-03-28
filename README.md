# Relationalize
This is a Python library for transforming collections of JSON objects into a relational-friendly format, compatible with MongoDB as a source. It is forked from [tulip/relationalize](https://github.com/tulip/relationalize), which draws inspiration from the [AWS Glue Relationalize transform](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-transforms-Relationalize.html).

This library differs from the original tulip/relationalize as follows:
- Added handling for JSON files of MongoDB records
- Added handling for Apache Flink SQL dialect
- Added PRIMARY KEY handling
- Modified naming conventions
- Modified and extended data type parsing
- Flexibility for array and object relationalization with the `ignore_arrays` and  `ignore_objects` arguments in the relationalize class constructor

## JSON Object Collections
When working with JSON often there are collections of objects with the same or similar structure. For example, in a NoSQL database there may be a collection describing users with the following two documents/objects:
```javascript
// Document 1
{
    "username": "jsmith123",
    "created_at": "Monday, December 15, 2022 at 20:24",
    "contact": {
        "email_address": "jsmith123@gmail.com",
        "phone_number": 1234567890
    },
    "connections": [
        "jdoe456",
        "elowry789"
    ],
    "visits": [
        {
            "seen_at": "2022-12-15T20:24:26.637Z",
            "count": 3
        }
    ]
}
// Document 2
{
    "username": "jdoe456",
    "created_at": 1671135896468,
    "contact": {
        "email_address": "jdoe899@yahoo.com",
        "address": {
            "address_1": "77 Middlesex Avenue",
            "address_2": "Suite A",
            "city": "Somerville",
            "state": "MA",
            "zip_code": "02145"
        }
    },
    "connections": [
        "jsmith123",
        "hjones99"
    ]
    "visits": []
}
```
There are a number of challenges that must be overcome to move this data into a relational-database friendly format:
1. Nested Objects (ex: "contact" field)
2. Different data types in the same column (ex: "created_at" field)
3. Sparse columns (ex: "contact.phone_number" & "contact.address" field)
4. Sub-Arrays (ex: "connections" & "visits" field)

This package provides a solution to all of these challenges with more portability and flexibility, and less limitations than AWS Glue relationalize.

## How Relationalize works
The relationalize function recursively navigates the JSON object and splits out new objects/collections whenever an array is encountered and provides a connection/relation between the objects. You provide the Relationalize class a function which will determine where to write the transformed content. This could be a local file object, a remote (s3) file object, or an in memory buffer. Additionally any nested objects are flattened. Each object that is output by relationalize is a flat JSON object.

This package also provides a `Schema` class which can generate a schema for a collection of flat JSON objects. This schema can be used to handle type ambigouity and generate SQL DDL.

For example, the schemas generated by relationalizing and schema generating the above collection's objects would be:
```javascript
// users
{
    "username": "str",
    "created_at": "c-int-str",
    "contact_email_address": "str",
    "contact_phone_number": "int",
    "contact_address_address_1": "str",
    "contact_address_address_2": "str",
    "contact_address_city": "str",
    "contact_address_state": "str",
    "contact_address_zip_code": "str",
    "connections": "str"
    "visits": "str"

}
// users_connections
{
    "_index_": "int",
    "_rid_": "str",
    "_val_": "str"
}
// users_visits
{
    "_index_": "int",
    "_rid_": "str",
    "seen_at": "datetime",
    "count": "str"
}
```

When processing a collection of JSON objects, the schema is not known, so we must provide a way for the relationalize class to store the new collections it will potentially create. This could be a local or remote file, an in memory buffer, etc...

The relationalize class constructor takes in a function with the signature `(identifier: str) -> TextIO` as an argument (`create_output`). This function is used to create the outputs.

The relationalize class constructor also takes in an optional function that will be called whenever an object is written to a file that was created via the `create_output` function. This method can be utilized to generate the schemas as the objects are encountered, reducing the number of iterations needed over the objects.

For example:
```python
schemas: Dict[str, Schema] = {}

def on_object_write(schema: str, object: dict):
  if schema not in schemas:
      schemas[schema] = Schema()
  schemas[schema].read_object(object)

with Relationalize('object_name', on_object_write=on_object_write) as r:
    r.relationalize([{...}, {...}])
```

Once the collection has been relationalized and the schemas have been generated, you can utilize the `convert_object` method to create the final json object, which could be loaded into a database. The `convert_object` method will break out any ambigously typed columns into separate columns.

For example the first document in the users collection would output the following three documents after being processed by `relationalize` and `convert_object`:
```javascript
// users
{
    "username": "jsmith123",
    "created_at": "Monday, December 15, 2022 at 20:24",
    "contact_email_address": "jsmith123@gmail.com",
    "contact_phone_number": 1234567890,
    "connections": "R_969c799a3177437d98074d985861242b"
    "visits": "R_121c799a3121437d98074d985861251a"
}
// users_connections
{
    "_index_": 0,
    "_rid_": "R_969c799a3177437d98074d985861242b",
    "_val_": "jdoe456"
}
{
    "_index_": 1,
    "_rid_": "R_969c799a3177437d98074d985861242b",
    "_val_": "elowry789"
}
// users_visits
{
    "_index_": 0,
    "_rid_": "R_121c799a3121437d98074d985861251a",
    "seen_at": "2022-12-15T20:24:26.637Z",
    "count": 3
}
```

### Options
The relationalize class constructor takes in a few optional arguments:
- The `ignore_arrays` boolean determines whether or not arrays are relationalized. By default (`False`), whenever an array is encountered, new tables are created and a connection/relation is provided between the objects. No action is taken when `ignore_arrays = True`.
- The `ignore_objects` boolean determines whether or not nested objects are relationalized. By default (`False`), nested keys are combined into a single key delimited by underscores. No action is taken when `ignore_objects = True`.
- See the [Logging section](#logging) to read about `log_level`.

For example:
```python
schemas: Dict[str, Schema] = {}

def on_object_write(schema: str, object: dict):
  if schema not in schemas:
      schemas[schema] = Schema()
  schemas[schema].read_object(object)

with Relationalize('object_name', on_object_write=on_object_write, ignore_arrays=True, ignore_objects=True) as r:
    r.relationalize([{...}, {...}])
```

With this, the first document in the users collection will output the following after being processed by `relationalize` and `convert_object`:
```javascript
// users
{
    "username": "jsmith123",
    "created_at": "Monday, December 15, 2022 at 20:24",
    "contact": {"email_address": "jsmith123@gmail.com", "phone_number": 1234567890},
    "connections": ["jdoe456","elowry789"],
    "visits": [{"seen_at": "2022-12-15T20:24:26.637Z","count": 3}]
}
```

## Logging
The relationalize and schema class constructor both have an optional `log_level` argument. It is set to `logging.WARNING` by default and outputs logs to the terminal.
```python
import logging

LOG_LVL = logging.DEBUG

def on_object_write(schema: str, object: dict):
  if schema not in schemas:
      schemas[schema] = Schema(log_level=LOG_LVL)
  schemas[schema].read_object(object)

with Relationalize('object_name', on_object_write=on_object_write, log_level=LOG_LVL) as r:
    r.relationalize([{...}, {...}])
```

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install relationalize.

```bash
pip install git+ssh://git@github.com/zenreach/relationalize.git@main#egg=relationalize
```
If you are making changes to this repo and want to see your changes during development in a parent repo, change `@main` to the desired branch. However, do not push this change in.

## Examples
Examples are placed in the `examples/` folder.
These examples are intended to be run from the working directory of `examples`.

We recommend starting with the `local_fs_example.py` and then moving to the `memory_example.py`.

For a complete API to database pipeline check out the `full_pokemon_s3_redshift_pipeline.py` example.

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
