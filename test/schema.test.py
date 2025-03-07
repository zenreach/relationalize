import unittest
from copy import deepcopy

from setup_tests import setup_tests

setup_tests()

from relationalize.schema import Schema

CASE_1 = {"1": 1, "2": "foobar", "3": False, "4": 1.2}

CASE_2 = {"1": "foobar", "2": 9.9, "3": True, "4": 9.5}

CASE_3 = {"1": None}
CASE_4 = {"1": 1}
CASE_5 = {"1": "foobar"}

CASE_6 = {"_id": "abc123", "not_id": "foobar"}

CASE_7 = {"1": "2017-06-30 22:38:59.051000", "2": "2017-11-12 22:38:59.011Z", "3": "2017-11-12 22:38:59.011-0500", "4": "2017-08-10T17:25:01.324+02:00", "5": "2017-07-09 00:00:00", "6": "2017-07-09T00:00:00", "7": "2017-07-09"}

# Field 1: int/float => int, Field 2: int/float => float
CASE_8A = {"1": 1, "2": 2}
CASE_8B = {"1": 1.0, "2": 2.2}


CASE_1_DDL = """
CREATE TABLE IF NOT EXISTS "public"."test" (
    "1" BIGINT
    , "2" VARCHAR(65535)
    , "3" BOOLEAN
    , "4" FLOAT
);
""".strip()

CASE_2_DDL = """
CREATE TABLE IF NOT EXISTS "public"."test" (
    "1_int" BIGINT
    , "1_str" VARCHAR(65535)
    , "2_float" FLOAT
    , "2_str" VARCHAR(65535)
    , "3" BOOLEAN
    , "4" FLOAT
);
""".strip()

CASE_6_DDL = """
CREATE TABLE IF NOT EXISTS "public"."test" (
    "_id" VARCHAR(65535) PRIMARY KEY
    , "not_id" VARCHAR(65535)
);
""".strip()

CASE_7_DDL = """
CREATE TABLE IF NOT EXISTS "public"."test" (
    "1" TIMESTAMPTZ
    , "2" TIMESTAMPTZ
    , "3" TIMESTAMPTZ
    , "4" TIMESTAMPTZ
    , "5" TIMESTAMPTZ
    , "6" TIMESTAMPTZ
    , "7" VARCHAR(65535)
);
""".strip()

class SchemaTest(unittest.TestCase):
    def test_all_types_no_choice(self):
        schema = Schema()
        schema.read_object(CASE_1)
        self.assertDictEqual(
            {"1": {"type": "int", "is_primary": False}, "2": {"type": "str", "is_primary": False}, "3": {"type": "bool", "is_primary": False}, "4": {"type": "float", "is_primary": False}}, 
            schema.schema
        )

    def test_basic_choice(self):
        schema = Schema()
        schema.read_object(CASE_1)
        schema.read_object(CASE_2)
        self.assertDictEqual(
            {"1": {"type": "c-int-str", "is_primary": False}, "2": {"type": "c-float-str", "is_primary": False}, "3": {"type": "bool", "is_primary": False}, "4": {"type": "float", "is_primary": False}},
            schema.schema,            
        )

    def test_primary_key(self):
        schema = Schema()
        schema.read_object(CASE_6)
        self.assertDictEqual(
            {"_id": {"type": "str", "is_primary": True}, "not_id": {"type": "str", "is_primary": False}}, 
            schema.schema
        )

    def test_datetime(self):
        schema = Schema()
        schema.read_object(CASE_7)
        self.assertDictEqual(
            {"1": {"type": "datetime", "is_primary": False}, "2": {"type": "datetime", "is_primary": False}, "3": {"type": "datetime", "is_primary": False}, "4": {"type": "datetime", "is_primary": False}, "5": {"type": "datetime", "is_primary": False}, "6": {"type": "datetime", "is_primary": False}, "7": {"type": "str", "is_primary": False}}, 
            schema.schema
        )
    
    def test_generalize_choice_int_float(self):
        schema = Schema()
        schema.read_object(CASE_8A)
        schema.read_object(CASE_8B)
        self.assertDictEqual(
            {"1": {"type": "int", "is_primary": False}, "2": {"type": "float", "is_primary": False}}, 
            schema.schema
        )

    def test_merge_noop(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)

        schema2 = Schema()
        schema2.read_object(CASE_1)

        schema3 = Schema()
        schema3.read_object(CASE_1)

        merged_schema = Schema.merge(schema1.schema, schema2.schema, schema3.schema)
        self.assertDictEqual(merged_schema.schema, schema1.schema)
        self.assertDictEqual(merged_schema.schema, schema2.schema)
        self.assertDictEqual(merged_schema.schema, schema3.schema)

    def test_merge_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)

        schema2 = Schema()
        schema2.read_object(CASE_2)

        merged_schema = Schema.merge(schema1.schema, schema2.schema)

        self.assertDictEqual(
            {"1": {"type": "c-int-str", "is_primary": False}, "2": {"type": "c-float-str", "is_primary": False}, "3": {"type": "bool", "is_primary": False}, "4": {"type": "float", "is_primary": False}},
            merged_schema.schema,
        )

    def test_merge_equal_parse(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)

        schema2 = Schema()
        schema2.read_object(CASE_2)

        merged_schema = Schema.merge(schema1.schema, schema2.schema)

        schema3 = Schema()
        schema3.read_object(CASE_1)
        schema3.read_object(CASE_2)

        self.assertDictEqual(merged_schema.schema, schema3.schema)

    def test_convert_object_no_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)

        converted1 = schema1.convert_object(deepcopy(CASE_1))
        self.assertDictEqual(converted1, CASE_1)

    def test_convert_object_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)
        schema1.read_object(CASE_2)

        converted1 = schema1.convert_object(deepcopy(CASE_1))
        self.assertDictEqual(
            {"1_int": 1, "2_str": "foobar", "3": False, "4": 1.2}, converted1
        )
        converted2 = schema1.convert_object(deepcopy(CASE_2))
        self.assertDictEqual(
            {"1_str": "foobar", "2_float": 9.9, "3": True, "4": 9.5}, converted2
        )

    def test_generate_ddl_no_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)
        self.assertEqual(CASE_1_DDL, schema1.generate_ddl("test"))

    def test_generate_ddl_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)
        schema1.read_object(CASE_2)

        self.assertEqual(CASE_2_DDL, schema1.generate_ddl("test"))

    def test_generate_ddl_primary_key(self):
        schema1 = Schema()
        schema1.read_object(CASE_6)
        self.assertEqual(CASE_6_DDL, schema1.generate_ddl("test"))

    def test_generate_ddl_datetime(self):
        schema1 = Schema()
        schema1.read_object(CASE_7)
        self.assertEqual(CASE_7_DDL, schema1.generate_ddl("test"))

    def test_none_cases(self):
        schema1 = Schema()
        schema1.read_object(CASE_3)
        self.assertDictEqual({"1": {"type": "none", "is_primary": False}}, schema1.schema)

        schema1.read_object(CASE_4)
        self.assertDictEqual({"1": {"type": "int", "is_primary": False}}, schema1.schema)

        schema1.read_object(CASE_5)
        self.assertDictEqual({"1": {"type": "c-int-str", "is_primary": False}}, schema1.schema)

        schema1.read_object(CASE_3)
        self.assertDictEqual({"1": {"type": "c-int-str", "is_primary": False}}, schema1.schema)

    def test_none_convert(self):
        schema1 = Schema()
        schema1.read_object(CASE_3)

        self.assertDictEqual({"1": None}, schema1.convert_object(CASE_3))

    def test_none_int_convert(self):
        schema1 = Schema()
        schema1.read_object(CASE_3)
        schema1.read_object(CASE_4)

        self.assertDictEqual({"1": None}, schema1.convert_object(CASE_3))
        self.assertDictEqual({"1": 1}, schema1.convert_object(CASE_4))

    def test_none_choice_convert(self):
        schema1 = Schema()
        schema1.read_object(CASE_3)
        schema1.read_object(CASE_4)
        schema1.read_object(CASE_5)

        self.assertDictEqual({"1": None}, schema1.convert_object(CASE_3))
        self.assertDictEqual({"1_int": 1}, schema1.convert_object(CASE_4))
        self.assertDictEqual({"1_str": "foobar"}, schema1.convert_object(CASE_5))

    def test_drop_null_columns(self):
        schema1 = Schema()
        schema1.read_object(CASE_3)
        self.assertDictEqual({"1": {"type": "none", "is_primary": False}}, schema1.schema)

        schema1.drop_null_columns()
        self.assertDictEqual({}, schema1.schema)

        schema2 = Schema()
        schema2.read_object(CASE_3)
        schema2.read_object(CASE_4)
        schema2.drop_null_columns()
        self.assertDictEqual({"1": {"type": "int", "is_primary": False}}, schema2.schema)

    def test_generate_output_columns_no_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)
        self.assertListEqual(["1", "2", "3", "4"], schema1.generate_output_columns())

    def test_generate_output_columns_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)
        schema1.read_object(CASE_2)
        self.assertListEqual(
            ["1_int", "1_str", "2_float", "2_str", "3", "4"],
            schema1.generate_output_columns(),
        )

    def test_drop_special_char_columns(self):
        schema1 = Schema()
        schema1.read_object({"abc ": 1, "def@#": 1, "$$ghi": 1, "jkl": 1, "!@#mno": 1})
        self.assertEqual(3, schema1.drop_special_char_columns())
        self.assertEqual(schema1.schema, {"abc ": {"type": "int", "is_primary": False}, "jkl": {"type": "int", "is_primary": False}})
        schema2 = Schema()
        schema2.read_object({"abc": 1, "def": 2, "GH I ": 3})
        self.assertEqual(0, schema2.drop_special_char_columns())
        self.assertEqual(schema2.schema, {"abc": {"type": "int", "is_primary": False}, "def": {"type": "int", "is_primary": False}, "GH I ": {"type": "int", "is_primary": False}})
        schema3 = Schema()
        schema3.read_object({"abc": 1, "de-f": 2, "GH_I ": 3})
        self.assertEqual(0, schema3.drop_special_char_columns())
        self.assertEqual(schema3.schema, {"abc": {"type": "int", "is_primary": False}, "de-f": {"type": "int", "is_primary": False}, "GH_I ": {"type": "int", "is_primary": False}})

    def test_drop_duplicate_columns(self):
        schema1 = Schema()
        schema1.read_object(
            {"ABc ": 1, "DEf ": 1, "ghi": 1, "jkl": 1, "ABC": 1, "abc ": 1, "JkL": 1}
        )
        self.assertEqual(2, schema1.drop_duplicate_columns())
        self.assertEqual(
            {"ABc ": {"type": "int", "is_primary": False}, "DEf ": {"type": "int", "is_primary": False}, "ghi": {"type": "int", "is_primary": False}, "jkl": {"type": "int", "is_primary": False}, "ABC": {"type": "int", "is_primary": False}},
            schema1.schema
        )
        schema2 = Schema()
        schema2.read_object(
            {"abc": 1, "ABC": 2, "ABc": 3, "abC ": 4, "D E F": 5, "DEF": 5}
        )
        self.assertEqual(2, schema2.drop_duplicate_columns())
        self.assertEqual(
            schema2.schema,
            {"abc": {"type": "int", "is_primary": False}, "abC ": {"type": "int", "is_primary": False}, "D E F": {"type": "int", "is_primary": False}, "DEF": {"type": "int", "is_primary": False}},
        )
        schema3 = Schema()
        schema3.read_object({"abc": 1, "def": 2, "GH I ": 3, "abC ": 4, "D E F": 5})
        self.assertEqual(0, schema3.drop_duplicate_columns())
        self.assertEqual(
            schema3.schema,
            {"abc": {"type": "int", "is_primary": False}, "def": {"type": "int", "is_primary": False}, "GH I ": {"type": "int", "is_primary": False}, "abC ": {"type": "int", "is_primary": False}, "D E F": {"type": "int", "is_primary": False}},
        )


if __name__ == "__main__":
    unittest.main()
