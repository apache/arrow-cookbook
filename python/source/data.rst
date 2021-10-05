=================
Data Manipulation
=================

Recipes related to filtering or transforming data in
arrays and tables.

.. contents::

See :ref:`compute` for a complete list of all available compute functions

Computing Mean/Min/Max values of an array
=========================================

Arrow provides compute functions that can be applied to arrays.
Those compute functions are exposed through the :mod:`arrow.compute`
module.

.. testsetup::

  import numpy as np
  import pyarrow as pa

  arr = pa.array(np.arange(100))

Given an array with 100 numbers, from 0 to 99

.. testcode::

  print(f"{arr[0]} .. {arr[-1]}")

.. testoutput::

  0 .. 99

We can compute the ``mean`` using the :func:`arrow.compute.mean`
function

.. testcode::

  import pyarrow.compute as pc

  mean = pc.mean(arr)
  print(mean)

.. testoutput::

  49.5

And the ``min`` and ``max`` using the :func:`arrow.compute.min_max`
function

.. testcode::

  import pyarrow.compute as pc

  min_max = pc.min_max(arr)
  print(min_max)

.. testoutput::

  [('min', 0), ('max', 99)]

Counting Occurrences of Elements
================================

Arrow provides compute functions that can be applied to arrays,
those compute functions are exposed through the :mod:`arrow.compute`
module.

.. testsetup::

  import pyarrow as pa

  nums_arr = pa.array(list(range(10))*10)

Given an array with all numbers from 0 to 9 repeated 10 times

.. testcode::

  print(f"LEN: {len(nums_arr)}, MIN/MAX: {nums_arr[0]} .. {nums_arr[-1]}")

.. testoutput::

  LEN: 100, MIN/MAX: 0 .. 9

We can count occurences of all entries in the array using the
:func:`arrow.compute.value_counts` function

.. testcode::

  import pyarrow.compute as pc

  counts = pc.value_counts(nums_arr)
  for pair in counts:
      print(pair)

.. testoutput::

  [('values', 0), ('counts', 10)]
  [('values', 1), ('counts', 10)]
  [('values', 2), ('counts', 10)]
  [('values', 3), ('counts', 10)]
  [('values', 4), ('counts', 10)]
  [('values', 5), ('counts', 10)]
  [('values', 6), ('counts', 10)]
  [('values', 7), ('counts', 10)]
  [('values', 8), ('counts', 10)]
  [('values', 9), ('counts', 10)]

Applying arithmetic functions to arrays.
=========================================

The compute functions in :mod:`arrow.compute` also include
common transformations such as arithmetic functions.

Given an array with 100 numbers, from 0 to 99

.. testcode::

  print(f"{arr[0]} .. {arr[-1]}")

.. testoutput::

  0 .. 99

We can multiply all values by 2 using the :func:`arrow.compute.multiply`
function

.. testcode::

  import pyarrow.compute as pc

  doubles = pc.multiply(arr, 2)
  print(f"{doubles[0]} .. {doubles[-1]}")

.. testoutput::

  0 .. 198

Appending tables to an existing table
=====================================

If you have data split across two different tables, it is possible
to concatenate their rows into a single table.

If we have the list of Oscar nominations divided between two different tables:

.. testcode::

  import pyarrow as pa

  oscar_nominations_1 = pa.table([
    ["Meryl Streep", "Katharine Hepburn"],
    [21, 12]
  ], names=["actor", "nominations"])

  oscar_nominations_2 = pa.table([
    ["Jack Nicholson", "Bette Davis"],
    [12, 10]
  ], names=["actor", "nominations"])

We can combine them into a single table using :func:`pyarrow.concat_tables`:

.. testcode::

  oscar_nominations = pa.concat_tables([oscar_nominations_1, 
                                        oscar_nominations_2])

  print(oscar_nominations.to_pydict())

.. testoutput::

  {'actor': ['Meryl Streep', 'Katharine Hepburn', 'Jack Nicholson', 'Bette Davis'], 'nominations': [21, 12, 12, 10]}

.. note::

  By default, appending two tables is a zero-copy operation that doesn't need to
  copy or rewrite data. As tables are made of :class:`pyarrow.ChunkedArray`,
  the result will be a table with multiple chunks, each pointing to the original 
  data that has been appended. Under some conditions, Arrow might have to 
  cast data from one type to another (if `promote=True`).  In such cases the data 
  will need to be copied and an extra cost will occur.

Searching for values matching a predicate in Arrays
===================================================

If you have to look for values matching a predicate in Arrow arrays
the :mod:`arrow.compute` module provides several methods that
can be used to find the values you are looking for.

For example, given an array with numbers from 0 to 9, if we
want to look only for those greater than 5 we could use the
func:`arrow.compute.greater` method and get back the elements
that fit our predicate

.. testcode::

  import pyarrow as pa
  import pyarrow.compute as pc

  arr = pa.array(range(10))
  gtfive = pc.greater(arr, 5)

  print(gtfive.to_string())

.. testoutput::

  [
    false,
    false,
    false,
    false,
    false,
    false,
    true,
    true,
    true,
    true
  ]

Furthermore we can filter the array to get only the entries
that match our predicate

.. testcode::

  filtered_array = pc.filter(arr, gtfive)
  print(filtered_array)

.. testoutput::

  [
    6,
    7,
    8,
    9
  ]