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