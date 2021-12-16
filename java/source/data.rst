=================
Data Manipulation
=================

Recipes related to compare, filtering or transforming data.

.. contents::


Compare fields on the array
===========================

.. literalinclude:: demo/src/main/java/Data.java
   :lines: 26-36
   :language: java

.. code-block::

    true
    false

Compare values on the array
===========================

.. literalinclude:: demo/src/main/java/Data.java
   :lines: 27-32, 38-57
   :language: java

.. code-block::

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
   :lines: 63-85
   :language: java

.. code-block::

    0
    1
    2
    ...
    98
    99
    -1

Binary search - O(log(n))
*************************

org.apache.arrow.algorithm.search.VectorSearcher#binarySearch - O(log(n))

.. literalinclude:: demo/src/main/java/Data.java
   :lines: 87, 64-78, 89-93
   :language: java

.. code-block::

    0
    1
    2
    ...
    98
    99
    -1

