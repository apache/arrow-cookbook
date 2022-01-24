=================
Data manipulation
=================

Recipes related to compare, filtering or transforming data.

.. contents::

We are going to use this util for data manipulation:

.. code-block:: java

Compare Vectors for Field Equality
==================================

.. testcode::

    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.compare.TypeEqualsVisitor;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    IntVector right = new IntVector("int", rootAllocator);
    right.allocateNew(3);
    right.set(0, 10);
    right.set(1, 20);
    right.set(2, 30);
    right.setValueCount(3);
    IntVector left1 = new IntVector("int", rootAllocator);
    IntVector left2 = new IntVector("int2", rootAllocator);
    TypeEqualsVisitor visitor = new TypeEqualsVisitor(right);

    System.out.println(visitor.equals(left1));
    System.out.println(visitor.equals(left2));

.. testoutput::

    true
    false



