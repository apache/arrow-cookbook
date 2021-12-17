======================
Creating arrow objects
======================

A vector is the basic unit in the java arrow columnar format.
Vectors are provided by java arrow for the interface FieldVector that extends ValueVector.


Array of int
============

.. literalinclude:: demo/src/main/java/Create.java
   :lines: 18-20
   :language: java

.. code-block::

    [1, 2, 3, 4, 5]


Array of varchar
================

.. literalinclude:: demo/src/main/java/Create.java
   :lines: 23-25
   :language: java

.. code-block::

    [david, gladis, juan]

Array of list
=============

.. literalinclude:: demo/src/main/java/Create.java
   :lines: 28-30
   :language: java

.. code-block::

    [[1,3,5,7,9], [2,4,6,8,10], [1,2,3,5,8]]

Code
====

.. literalinclude:: demo/src/main/java/Create.java
   :language: java
