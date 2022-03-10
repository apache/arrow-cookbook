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
  print(oscar_nominations)

.. testoutput::

    pyarrow.Table
    actor: string
    nominations: int64
    ----
    actor: [["Meryl Streep","Katharine Hepburn"],["Jack Nicholson","Bette Davis"]]
    nominations: [[21,12],[12,10]]

.. note::

  By default, appending two tables is a zero-copy operation that doesn't need to
  copy or rewrite data. As tables are made of :class:`pyarrow.ChunkedArray`,
  the result will be a table with multiple chunks, each pointing to the original 
  data that has been appended. Under some conditions, Arrow might have to 
  cast data from one type to another (if `promote=True`).  In such cases the data 
  will need to be copied and an extra cost will occur.

Adding a column to an existing Table
====================================

If you have a table it is possible to extend its columns using
:meth:`pyarrow.Table.append_column`

Suppose we have a table with oscar nominations for each actress

.. testcode::

  import pyarrow as pa

  oscar_nominations = pa.table([
    ["Meryl Streep", "Katharine Hepburn"],
    [21, 12]
  ], names=["actor", "nominations"])

  print(oscar_nominations)

.. testoutput::

    pyarrow.Table
    actor: string
    nominations: int64
    ----
    actor: [["Meryl Streep","Katharine Hepburn"]]
    nominations: [[21,12]]

it's possible to append an additional column to track the years the
nomination was won using :meth:`pyarrow.Table.append_column`

.. testcode::

  oscar_nominations = oscar_nominations.append_column(
    "wonyears", 
    pa.array([
      [1980, 1983, 2012],
      [1934, 1968, 1969, 1982]
    ])
  )

  print(oscar_nominations)

.. testoutput::

    pyarrow.Table
    actor: string
    nominations: int64
    wonyears: list<item: int64>
      child 0, item: int64
    ----
    actor: [["Meryl Streep","Katharine Hepburn"]]
    nominations: [[21,12]]
    wonyears: [[[1980,1983,2012],[1934,1968,1969,1982]]]


Replacing a column in an existing Table
=======================================

If you have a table it is possible to replace an existing column using
:meth:`pyarrow.Table.set_column`

Suppose we have a table with information about items sold at a supermarket
on a particular day.

.. testcode::

  import pyarrow as pa

  sales_data = pa.table([
    ["Potato", "Bean", "Cucumber", "Eggs"],
    [21, 12, 10, 30]
  ], names=["item", "amount"])

  print(sales_data)

.. testoutput::

    pyarrow.Table
    item: string
    amount: int64
    ----
    item: [["Potato","Bean","Cucumber","Eggs"]]
    amount: [[21,12,10,30]]

it's possible to replace the existing column `amount`
in index `1` to update the sales 
using :meth:`pyarrow.Table.set_column`

.. testcode::

  new_sales_data = sales_data.set_column(
    1, 
    "new_amount",
    pa.array([30, 20, 15, 40])
  )

  print(new_sales_data)

.. testoutput::

    pyarrow.Table
    item: string
    new_amount: int64
    ----
    item: [["Potato","Bean","Cucumber","Eggs"]]
    new_amount: [[30,20,15,40]]

.. data_group_a_table:

Group a Table
================

If you have a table which needs to be grouped by a particular key, 
you can use :meth:`pyarrow.Table.group_by` followed by an aggregation
operation :meth:`pyarrow.TableGroupBy.aggregate`. Learn more about
groupby operations `here <https://arrow.apache.org/docs/python/compute.html#grouped-aggregations>`_.

For example, let’s say we have some data with a particular set of keys
and values associated with that key. And we want to group the data by 
those keys and apply an aggregate function like sum to evaluate
how many items are for each unique key. 

.. testcode::

  import pyarrow as pa

  table = pa.table([
       pa.array(["a", "a", "b", "b", "c", "d", "e", "c"]),
       pa.array([11, 20, 3, 4, 5, 1, 4, 10]),
      ], names=["keys", "values"])

  print(table)

.. testoutput::

    pyarrow.Table
    keys: string
    values: int64
    ----
    keys: [["a","a","b","b","c","d","e","c"]]
    values: [[11,20,3,4,5,1,4,10]]

Now we let's apply a groupby operation. The table will be grouped
by the field ``key`` and an aggregation operation, ``sum`` is applied
on the column ``values``. Note that, an aggregation operation pairs with a column name. 

.. testcode::

  aggregated_table = table.group_by("keys").aggregate([("values", "sum")])

  print(aggregated_table)

.. testoutput::

    pyarrow.Table
    values_sum: int64
    keys: string
    ----
    values_sum: [[31,7,15,1,4]]
    keys: [["a","b","c","d","e"]]

If you observe carefully, the new table returns the aggregated column
as ``values_sum`` which is formed by the column name and aggregation operation name. 

Aggregation operations can be applied with options. Let's take a case where
we have null values included in our dataset, but we want to take the 
count of the unique groups excluding the null values. 

A sample dataset can be formed as follows. 

.. testcode::

  import pyarrow as pa

  table = pa.table([
        pa.array(["a", "a", "b", "b", "b", "c", "d", "d", "e", "c"]),
        pa.array([None, 20, 3, 4, 5, 6, 10, 1, 4, None]),
        ], names=["keys", "values"])

  print(table)

.. testoutput::

    pyarrow.Table
    keys: string
    values: int64
    ----
    keys: [["a","a","b","b","b","c","d","d","e","c"]]
    values: [[null,20,3,4,5,6,10,1,4,null]]

Let's apply an aggregation operation ``count`` with the option to exclude
null values. 

.. testcode::

  import pyarrow.compute as pc

  grouped_table = table.group_by("keys").aggregate(
    [("values", 
    "count",
    pc.CountOptions(mode="only_valid"))]
  )

  print(grouped_table)

.. testoutput::

    pyarrow.Table
    values_count: int64
    keys: string
    ----
    values_count: [[1,3,1,2,1]]
    keys: [["a","b","c","d","e"]]


Sort a Table
============

Let's discusse how to sort a table. We can sort a table, 
based on values of a given column. Data can be either sorted ``ascending`` 
or ``descending``. 

Prepare data;

.. testcode::

  import pyarrow as pa

  table = pa.table([
        pa.array(["a", "a", "b", "b", "b", "c", "d", "d", "e", "c"]),
        pa.array([15, 20, 3, 4, 5, 6, 10, 1, 14, 123]),
        ], names=["keys", "values"])

  print(table)

.. testoutput::

    pyarrow.Table
    keys: string
    values: int64
    ----
    keys: [["a","a","b","b","b","c","d","d","e","c"]]
    values: [[15,20,3,4,5,6,10,1,14,123]]

Then applying sort;

.. testcode::

  sorted_table = table.sort_by([("values", "ascending")])

  print(sorted_table)

.. testoutput::

    pyarrow.Table
    keys: string
    values: int64
    ----
    keys: [["d","b","b","b","c","d","e","a","a","c"]]
    values: [[1,3,4,5,6,10,14,15,20,123]]


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

Filtering Arrays using a mask
=============================

In many cases, when you are searching for something in an array
you will end up with a mask that tells you the positions at which
your search matched the values.

For example in an array of four items, we might have a mask that
matches the first and the last items only:

.. testcode::

  import pyarrow as pa

  array = pa.array([1, 2, 3, 4])
  mask = pa.array([True, False, False, True])

We can then filter the array according to the mask using
:meth:`pyarrow.Array.filter` to get back a new array with
only the values matching the mask:

.. testcode::

  filtered_array = array.filter(mask)
  print(filtered_array)

.. testoutput::

  [
    1,
    4
  ]

Most search functions in :mod:`pyarrow.compute` will produce
a mask as the output, so you can use them to filter your arrays
for the values that have been found by the function.

For example we might filter our arrays for the values equal to ``2``
using :func:`pyarrow.compute.equal`:

.. testcode::

  import pyarrow.compute as pc

  filtered_array = array.filter(pc.equal(array, 2))
  print(filtered_array)

.. testoutput::

  [
    2
  ]
