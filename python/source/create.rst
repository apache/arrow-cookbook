======================
Creating Arrow Objects
======================

Recipes related to the creation of Arrays, Tables,
Tensors and all other Arrow entities.

.. contents::

Creating Tables
===============

Arrow supports tabular data in :class:`pyarrow.Table`, each column
is represented by a :class:`pyarrow.Array` and tables can be created
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

Create Table from Plain Types
=============================

Arrow allows fast zero copy creation of arrow arrays
from numpy and pandas arrays and series, but it's also
possible to create Arrow Arrays and Tables from 
plain Python structures.

the :func:`pyarrow.table` function allows creation of Tables
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

.. note::

    All values provided in the dictionary will be passed to
    :func:`pyarrow.array` for conversion to Arrow arrays,
    and will benefit from zero copy behaviour when possible.

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
