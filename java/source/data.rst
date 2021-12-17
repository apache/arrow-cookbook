=================
Data manipulation
=================

Recipes related to compare, filtering or transforming data.

.. contents::


Compare fields on the array
===========================

.. literalinclude:: demo/src/main/java/Data.java
   :lines: 23-34
   :language: java

.. code-block::

    Comparing vector fields:
    true
    false

Compare values on the array
===========================

.. literalinclude:: demo/src/main/java/Data.java
   :lines: 36-55
   :language: java

.. code-block::

    Comparing two values at the given indices in the vectors:
    true
    true
    true
    true
    true
    true

Search values on the array
==========================

Linear search - O(n)
********************

org.apache.arrow.algorithm.search.VectorSearcher#linearSearch - O(n)

.. literalinclude:: demo/src/main/java/Data.java
   :lines: 57-80
   :language: java

.. code-block::

    Linear search:
    0
    1
    2
    3
    4
    5
    6
    7
    8
    9
    -1

Binary search - O(log(n))
*************************

org.apache.arrow.algorithm.search.VectorSearcher#binarySearch - O(log(n))

.. literalinclude:: demo/src/main/java/Data.java
   :lines: 82, 59-64, 84-91
   :language: java

.. code-block::

    Binary search:
    0
    1
    2
    3
    4
    5
    6
    7
    8
    9
    -1

Sort values on the array
========================

In-place sorter - O(nlog(n))
****************************

Sorting by manipulating the original vector.
org.apache.arrow.algorithm.sort.FixedWidthInPlaceVectorSorter - O(nlog(n))

.. literalinclude:: demo/src/main/java/Data.java
   :lines: 93-124
   :language: java

.. code-block::

    Sort the vector - In-place sorter:
    true
    true
    true
    true
    true
    true
    true
    true
    true
    true

Out-place sorter - O(nlog(n))
*****************************

Sorting by copies vector elements to a new vector in sorted order - O(nlog(n))
org.apache.arrow.algorithm.sort.FixedWidthInPlaceVectorSorter.
FixedWidthOutOfPlaceVectorSorter & VariableWidthOutOfPlaceVectorSor

.. literalinclude:: demo/src/main/java/Data.java
   :lines: 126-160
   :language: java

.. code-block::

    Sort the vector - Out-of-place sorter:
    true
    true
    true
    true
    true
    true
    true
    true
    true
    true
    true

Code
====

.. literalinclude:: demo/src/main/java/Data.java
   :language: java
