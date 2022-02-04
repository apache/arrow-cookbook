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

Array of List
-------------

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.complex.impl.UnionListWriter;
    import org.apache.arrow.vector.complex.ListVector;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    ListVector listVector = ListVector.empty("listVector", rootAllocator);
    UnionListWriter listWriter = listVector.getWriter();
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

.. testoutput::

    [[1,2,3], [10,20,30], [100,200,300], [1000,2000,3000]]

Creating VectorSchemaRoot (Table)
=================================

A `VectorSchemaRoot <https://arrow.apache.org/docs/java/vector_schema_root.html>`_
is a container that can hold batches, batches flow through VectorSchemaRoot as part of a pipeline.

.. testcode::

	import org.apache.arrow.memory.RootAllocator;
	import org.apache.arrow.vector.VarCharVector;
	import org.apache.arrow.vector.IntVector;
	import org.apache.arrow.vector.types.pojo.Field;
	import org.apache.arrow.vector.types.pojo.FieldType;
	import org.apache.arrow.vector.types.pojo.ArrowType;
	import org.apache.arrow.vector.types.pojo.Schema;
	import org.apache.arrow.vector.VectorSchemaRoot;
	import static java.util.Arrays.asList;

	// create a column data type
	Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
	Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

	// create a definition
	Schema schemaPerson = new Schema(asList(name, age));
	RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
	VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);

	// getting field vectors
	VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
	nameVector.allocateNew(3);
	nameVector.set(0, "david".getBytes());
	nameVector.set(1, "gladis".getBytes());
	nameVector.set(2, "juan".getBytes());
	nameVector.setValueCount(3);
	IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
	ageVector.allocateNew(3);
	ageVector.set(0, 10);
	ageVector.set(1, 20);
	ageVector.set(2, 30);
	ageVector.setValueCount(3);

	vectorSchemaRoot.setRowCount(3);

	System.out.print(vectorSchemaRoot.contentToTSVString());

.. testoutput::

    name    age
    david    10
    gladis    20
    juan    30
