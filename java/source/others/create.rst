.. _arrow-create:

======================
Creating Arrow Objects
======================

| A vector is the basic unit in the Arrow Java library. Vector by definition is intended to be mutable, a Vector can be changed it is mutable.

| Vectors are provided by java arrow for the interface `FieldVector <https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/FieldVector.html>`_ that extends `ValueVector <https://arrow.apache.org/docs/java/vector.html>`_.

.. contents::

Creating Vectors (arrays)
=========================

Array of Int (32-bit integer value)
-----------------------------------

.. testcode::

   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.memory.RootAllocator;

   RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

   IntVector intVector = new IntVector("intVector", rootAllocator);
   intVector.allocateNew(3);
   intVector.set(0, 1);
   intVector.set(1, 2);
   intVector.set(2, 3);
   intVector.setValueCount(3);

   System.out.print(intVector);

.. testoutput::

    [1, 2, 3]

Array of Varchar
----------------

.. testcode::

    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

    VarCharVector varVector = new VarCharVector("varVector", rootAllocator);
    varVector.allocateNew(3);
    varVector.set(0, "one".getBytes());
    varVector.set(1, "two".getBytes());
    varVector.set(2, "three".getBytes());
    varVector.setValueCount(3);

    System.out.print(varVector);

.. testoutput::

    [one, two, three]