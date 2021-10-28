===================
Working with Schema
===================

Arrow automatically infers the most appropriate data type when reading in data
or converting Python objects to Arrow objects.  

However, you might want to manually tell Arrow which data types to 
use, for example, to ensure interoperability with databases and data warehouse 
systems.  This chapter includes recipes for dealing with schemas.

.. contents::

Setting the data type of an Arrow Array
=======================================

If you have an existing array and want to change its data type,
that can be done through the ``cast`` function:

.. testcode::

    import pyarrow as pa

    arr = pa.array([1, 2, 3, 4, 5])
    print(arr.type)

.. testoutput::

    int64

.. testcode::

    arr = arr.cast(pa.int8())
    print(arr.type)

.. testoutput::

    int8

You can also create an array of the requested type by providing
the type at array creation

.. testcode::

    import pyarrow as pa

    arr = pa.array([1, 2, 3, 4, 5], type=pa.int8())
    print(arr.type)

.. testoutput::

    int8

Setting the schema of a Table
=============================

Tables detain multiple columns, each with its own name
and type. The union of types and names is what defines a schema.

A schema in Arrow can be defined using :meth:`pyarrow.schema`

.. testcode::

    import pyarrow as pa

    schema = pa.schema([
        ("col1", pa.int8()),
        ("col2", pa.string()),
        ("col3", pa.float64())
    ])

The schema can then be provided to a table when created:

.. testcode::

    table = pa.table([
        [1, 2, 3, 4, 5],
        ["a", "b", "c", "d", "e"],
        [1.0, 2.0, 3.0, 4.0, 5.0]
    ], schema=schema)

    print(table)

.. testoutput::

    pyarrow.Table
    col1: int8
    col2: string
    col3: double
    ----
    col1: [[1,2,3,4,5]]
    col2: [["a","b","c","d","e"]]
    col3: [[1,2,3,4,5]]

Like for arrays, it's possible to cast tables to different schemas
as far as they are compatible

.. testcode::

    schema_int32 = pa.schema([
        ("col1", pa.int32()),
        ("col2", pa.string()),
        ("col3", pa.float64())
    ])

    table = table.cast(schema_int32)

    print(table)

.. testoutput::

    pyarrow.Table
    col1: int32
    col2: string
    col3: double
    ----
    col1: [[1,2,3,4,5]]
    col2: [["a","b","c","d","e"]]
    col3: [[1,2,3,4,5]]

Merging multiple schemas
========================

When you have multiple separate groups of data that you want to combine
it might be necessary to unify their schemas to create a superset of them
that applies to all data sources.

.. testcode::

    import pyarrow as pa

    first_schema = pa.schema([
        ("country", pa.string()),
        ("population", pa.int32())
    ])

    second_schema = pa.schema([
        ("country_code", pa.string()),
        ("language", pa.string())
    ])

:func:`unify_schemas` can be used to combine multiple schemas into
a single one:

.. testcode::

    union_schema = pa.unify_schemas([first_schema, second_schema])

    print(union_schema)

.. testoutput::

    country: string
    population: int32
    country_code: string
    language: string

If the combined schemas have overlapping columns, they can still be combined
as far as the colliding columns retain the same type (``country_code``):

.. testcode::

    third_schema = pa.schema([
        ("country_code", pa.string()),
        ("lat", pa.float32()),
        ("long", pa.float32()),
    ])

    union_schema =  pa.unify_schemas([first_schema, second_schema, third_schema])

    print(union_schema)

.. testoutput::

    country: string
    population: int32
    country_code: string
    language: string
    lat: float
    long: float

If a merged field has instead diverging types in the combined schemas
then trying to merge the schemas will fail. For example if ``country_code``
was a numeric instead of a string we would be unable to unify the schemas
because in ``second_schema`` it was already declared as a ``pa.string()``

.. testcode::

    third_schema = pa.schema([
        ("country_code", pa.int32()),
        ("lat", pa.float32()),
        ("long", pa.float32()),
    ])

    try:
        union_schema =  pa.unify_schemas([first_schema, second_schema, third_schema])
    except pa.ArrowInvalid as e:
        print(e)

.. testoutput::

    Unable to merge: Field country_code has incompatible types: string vs int32