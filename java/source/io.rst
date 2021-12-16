========================
Reading and Writing Data
========================

Recipes related to reading and writing data from disk using
Apache Arrow.

.. contents::

Saving array
============

It is possible to dump data in the raw arrow format which allows 
direct memory mapping of data from disk. This format is called
the Arrow IPC format. There are two option: Random access format
& Streaming format.

Saving arrays with the IPC file format
**************************************

Random access to file:

.. literalinclude:: demo/src/main/java/Io.java
   :lines: 63-70
   :language: java

Random acces to buffer:

.. literalinclude:: demo/src/main/java/Io.java
   :lines: 63, 72-77
   :language: java


Saving arrays with the IPC Streamed Format
******************************************

Streaming to file:

.. literalinclude:: demo/src/main/java/Io.java
   :lines: 96-103
   :language: java

Streaming to buffer:

.. literalinclude:: demo/src/main/java/Io.java
   :lines: 96, 105-110
   :language: java



Read array
==========

Arrow vectors that have been written to disk in the Arrow IPC
format can be memory mapped back directly from the disk. There 
are two option: Random access format & Streaming format

Read arrays with the IPC file format
************************************

Random access to file:

.. literalinclude:: demo/src/main/java/Io.java
   :lines: 79-86
   :language: java

Random acces to buffer:

.. literalinclude:: demo/src/main/java/Io.java
   :lines: 88-94
   :language: java


Read arrays with the IPC Streamed Format
****************************************

Streaming to file:

.. literalinclude:: demo/src/main/java/Io.java
   :lines: 112-116
   :language: java

Streaming to buffer:

.. literalinclude:: demo/src/main/java/Io.java
   :lines: 121-124
   :language: java

Recipe
======

Code:

.. literalinclude:: demo/src/main/java/Io.java
   :language: java

Output:

.. code-block::

    name    document    age points
    david   A   10  [1,3,5,7,9]
    gladis  B   20  [2,4,6,8,10]
    juan    C   30  [1,2,3,5,8]

