======================
Creating Arrow Objects
======================

A vector is the basic unit in the java arrow columnar format.
Vectors are provided by java arrow for the interface FieldVector that extends ValueVector.


Int
===

.. literalinclude:: demo/src/main/java/Create.java
   :lines: 18-20
   :language: java

.. code-block::

    [1, 2, 3, 4, 5]


Varchar
=======

.. literalinclude:: demo/src/main/java/Create.java
   :lines: 23-25
   :language: java

.. code-block::

    [david, gladis, juan]

Array
=====

.. literalinclude:: demo/src/main/java/Create.java
   :lines: 28-30
   :language: java

.. code-block::

    [[1,3,5,7,9], [2,4,6,8,10], [1,2,3,5,8]]

Recipe
======

Code:

.. literalinclude:: demo/src/main/java/Create.java
   :language: java
