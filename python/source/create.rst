.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

======================
Creating Arrow Objects
======================

Recipes related to the creation of Arrays, Tables,
Tensors and all other Arrow entities.

.. contents::

Creating Arrays
===============

Arrow keeps data in continuous arrays optimised for memory footprint
and SIMD analyses. In Python it's possible to build :class:`pyarrow.Array`
starting from Python ``lists`` (or sequence types in general),
``numpy`` arrays and ``pandas`` Series.

.. testcode::

    import pyarrow as pa

    array = pa.array([1, 2, 3, 4, 5])

.. testcode::

    print(array)

.. testoutput::

    [
      1,
      2,
      3,
      4,
      5
    ]

Arrays can also provide a ``mask`` to specify which values should
be considered nulls

.. testcode::

    import numpy as np

    array = pa.array([1, 2, 3, 4, 5],
                     mask=np.array([True, False, True, False, True]))

    print(array)

.. testoutput::

    [
      null,
      2,
      null,
      4,
      null
    ]

When building arrays from ``numpy`` or ``pandas``, Arrow will leverage
optimized code paths that rely on the internal in-memory representation
of the data by ``numpy`` and ``pandas``

.. testcode::

    import numpy as np
    import pandas as pd

    array_from_numpy = pa.array(np.arange(5))
    array_from_pandas = pa.array(pd.Series([1, 2, 3, 4, 5]))

Creating Tables
===============

Arrow supports tabular data in :class:`pyarrow.Table`: each column
is represented by a :class:`pyarrow.ChunkedArray` and tables can be created
by pairing multiple arrays with names for their columns

.. testcode::

    import pyarrow as pa

    table = pa.table([
        pa.array([1, 2, 3, 4, 5]),
        pa.array(["a", "b", "c", "d", "e"]),
        pa.array([1.0, 2.0, 3.0, 4.0, 5.0])
    ], names=["col1", "col2", "col3"])

    print(table)

.. testoutput::

    pyarrow.Table
    col1: int64
    col2: string
    col3: double
    ----
    col1: [[1,2,3,4,5]]
    col2: [["a","b","c","d","e"]]
    col3: [[1,2,3,4,5]]

Create Table from Plain Types
=============================

Arrow allows fast zero copy creation of arrow arrays
from numpy and pandas arrays and series, but it's also
possible to create Arrow Arrays and Tables from
plain Python structures.

The :func:`pyarrow.table` function allows creation of Tables
from a variety of inputs, including plain python objects

.. testcode::

    import pyarrow as pa

    table = pa.table({
        "col1": [1, 2, 3, 4, 5],
        "col2": ["a", "b", "c", "d", "e"]
    })

    print(table)

.. testoutput::

    pyarrow.Table
    col1: int64
    col2: string
    ----
    col1: [[1,2,3,4,5]]
    col2: [["a","b","c","d","e"]]

.. note::

    All values provided in the dictionary will be passed to
    :func:`pyarrow.array` for conversion to Arrow arrays,
    and will benefit from zero copy behaviour when possible.

The :meth:`pyarrow.Table.from_pylist` method allows the creation
of Tables from python lists of row dicts. Types are inferred if a
schema is not explicitly passed.

.. testcode::

    import pyarrow as pa

    table = pa.Table.from_pylist([
        {"col1": 1, "col2": "a"},
        {"col1": 2, "col2": "b"},
        {"col1": 3, "col2": "c"},
        {"col1": 4, "col2": "d"},
        {"col1": 5, "col2": "e"}
    ])

    print(table)

.. testoutput::

    pyarrow.Table
    col1: int64
    col2: string
    ----
    col1: [[1,2,3,4,5]]
    col2: [["a","b","c","d","e"]]

Creating Record Batches
=======================

Most I/O operations in Arrow happen when shipping batches of data
to their destination.  :class:`pyarrow.RecordBatch` is the way
Arrow represents batches of data.  A RecordBatch can be seen as a slice
of a table.

.. testcode::

    import pyarrow as pa

    batch = pa.RecordBatch.from_arrays([
        pa.array([1, 3, 5, 7, 9]),
        pa.array([2, 4, 6, 8, 10])
    ], names=["odd", "even"])

Multiple batches can be combined into a table using
:meth:`pyarrow.Table.from_batches`

.. testcode::

    second_batch = pa.RecordBatch.from_arrays([
        pa.array([11, 13, 15, 17, 19]),
        pa.array([12, 14, 16, 18, 20])
    ], names=["odd", "even"])

    table = pa.Table.from_batches([batch, second_batch])

.. testcode::

    print(table)

.. testoutput::

    pyarrow.Table
    odd: int64
    even: int64
    ----
    odd: [[1,3,5,7,9],[11,13,15,17,19]]
    even: [[2,4,6,8,10],[12,14,16,18,20]]

Equally, :class:`pyarrow.Table` can be converted to a list of
:class:`pyarrow.RecordBatch` using the :meth:`pyarrow.Table.to_batches`
method

.. testcode::

    record_batches = table.to_batches(max_chunksize=5)
    print(len(record_batches))

.. testoutput::

    2

Store Categorical Data
======================

Arrow provides the :class:`pyarrow.DictionaryArray` type
to represent categorical data without the cost of
storing and repeating the categories over and over.  This can reduce memory use
when columns might have large values (such as text).

If you have an array containing repeated categorical data,
it is possible to convert it to a :class:`pyarrow.DictionaryArray`
using :meth:`pyarrow.Array.dictionary_encode`

.. testcode::

    arr = pa.array(["red", "green", "blue", "blue", "green", "red"])

    categorical = arr.dictionary_encode()
    print(categorical)

.. testoutput::

    ...
    -- dictionary:
      [
        "red",
        "green",
        "blue"
      ]
    -- indices:
      [
        0,
        1,
        2,
        2,
        1,
        0
      ]

If you already know the categories and indices then you can skip the encode
step and directly create the ``DictionaryArray`` using
:meth:`pyarrow.DictionaryArray.from_arrays`

.. testcode::

    categorical = pa.DictionaryArray.from_arrays(
        indices=[0, 1, 2, 2, 1, 0],
        dictionary=["red", "green", "blue"]
    )
    print(categorical)

.. testoutput::

    ...
    -- dictionary:
      [
        "red",
        "green",
        "blue"
      ]
    -- indices:
      [
        0,
        1,
        2,
        2,
        1,
        0
      ]
