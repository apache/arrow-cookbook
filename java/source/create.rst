.. _arrow-create:

======================
Creating Arrow Objects
======================

A vector is the basic unit in the Arrow Java library. Data types
describe the types of values; ValueVectors are sequences of typed
values. Vectors represent a one-dimensional sequence of values of
the same type. They are mutable containers.

Vectors implement the interface `ValueVector`_. The Arrow libraries provide
implementations of vectors for various data types.

.. contents::

Creating Vectors (arrays)
=========================

Array of Int
------------

.. testcode::

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.IntVector;

    try(
        BufferAllocator allocator = new RootAllocator();
        IntVector intVector = new IntVector("intVector", allocator)
    ) {
        intVector.allocateNew(3);
        intVector.set(0, 1);
        intVector.set(1, 2);
        intVector.set(2, 3);
        intVector.setValueCount(3);

        System.out.print(intVector);
    }

.. testoutput::

    [1, 2, 3]


Array of Varchar
----------------

.. testcode::

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;

    try(
        BufferAllocator allocator = new RootAllocator();
        VarCharVector varCharVector = new VarCharVector("varCharVector", allocator);
    ) {
        varCharVector.allocateNew(3);
        varCharVector.set(0, "one".getBytes());
        varCharVector.set(1, "two".getBytes());
        varCharVector.set(2, "three".getBytes());
        varCharVector.setValueCount(3);

        System.out.print(varCharVector);
    }

.. testoutput::

    [one, two, three]

Array of List
-------------

.. testcode::

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.complex.impl.UnionListWriter;
    import org.apache.arrow.vector.complex.ListVector;

    try(
        BufferAllocator allocator = new RootAllocator();
        ListVector listVector = ListVector.empty("listVector", allocator);
        UnionListWriter listWriter = listVector.getWriter()
    ) {
        int[] data = new int[] { 1, 2, 3, 10, 20, 30, 100, 200, 300, 1000, 2000, 3000 };
        int tmp_index = 0;
        for(int i = 0; i < 4; i++) {
            listWriter.setPosition(i);
            listWriter.startList();
            for(int j = 0; j < 3; j++) {
                listWriter.writeInt(data[tmp_index]);
                tmp_index = tmp_index + 1;
            }
            listWriter.setValueCount(3);
            listWriter.endList();
        }
        listVector.setValueCount(4);

        System.out.print(listVector);
    } catch (Exception e) {
        e.printStackTrace();
    }

.. testoutput::

    [[1,2,3], [10,20,30], [100,200,300], [1000,2000,3000]]

.. _`FieldVector`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/FieldVector.html
.. _`ValueVector`: https://arrow.apache.org/docs/java/vector.html