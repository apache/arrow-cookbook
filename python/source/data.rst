=================
Data Manipulation
=================

Recipes related to how to filter or transform data in
arrays and tables.

.. contents::

Computing Mean/Min/Max values of an array
=========================================

Arrow provides compute functions that can be applied to arrays,
those compute functions are exposed through the :mod:`arrow.compute`
module.

.. testsetup::

  import numpy as np
  import pyarrow as pa

  arr = pa.array(np.arange(100))

Given an array with all numbers from 0 to 100

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

  {'min': 0, 'max': 99}

Counting Occurrences of Elements
================================

Arrow provides compute functions that can be applied to arrays,
those compute functions are exposed through the :mod:`arrow.compute`
module.

.. testsetup::

  import pyarrow as pa

  nums_arr = pa.array(list(range(10))*10)

Given an array with all numbers from 0 to 10 repeated 10 times

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

  {'values': 0, 'counts': 10}
  {'values': 1, 'counts': 10}
  {'values': 2, 'counts': 10}
  {'values': 3, 'counts': 10}
  {'values': 4, 'counts': 10}
  {'values': 5, 'counts': 10}
  {'values': 6, 'counts': 10}
  {'values': 7, 'counts': 10}
  {'values': 8, 'counts': 10}
  {'values': 9, 'counts': 10}

Applying arithmetic functions to arrays.
=========================================

The compute functions in :mod:`arrow.compute` also include
most common transformation functions, like airthmetic ones.

Given an array with all numbers from 0 to 100

.. testcode::

  print(f"{arr[0]} .. {arr[-1]}")

.. testoutput::

  0 .. 99

We can multiple all values by 2 using the :func:`arrow.compute.multiply`
function

.. testcode::

  import pyarrow.compute as pc

  doubles = pc.multiply(arr, 2)
  print(f"{doubles[0]} .. {doubles[-1]}")

.. testoutput::

  0 .. 198
