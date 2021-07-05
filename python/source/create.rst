======================
Creating Arrow Objects
======================

Recipes related to how to create Arrays, Tables,
Tensors and all other Arrow entities.

.. contents::

Create Table from plain types
=============================

Arrow allows fast zero copy creation of arrow arrays
from numpy and pandas arrays and series, but it's also
possible to create Arrow arrays and Tables from 
plain Python structures.

the :func:`pyarrow.table` function allows creation of Tables
from nearly all supported inputs, including plain python objects

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