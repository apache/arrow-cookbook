.. _arrow-create:

======================
Creating Arrow Objects
======================

| A vector is the basic unit in the Arrow Java library. Vector by definition is intended to be mutable, a Vector can be changed it is mutable.

| Vectors are provided by java arrow for the interface `FieldVector <https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/FieldVector.html>`_ that extends `ValueVector <https://arrow.apache.org/docs/java/vector.html>`_.

.. contents::

We are going to use this util for creating arrow objects:

.. code-block:: java

   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.BitVectorHelper;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.vector.VarCharVector;
   import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
   import org.apache.arrow.vector.complex.ListVector;
   import org.apache.arrow.vector.types.Types;
   import org.apache.arrow.vector.types.pojo.FieldType;

   import java.util.List;


   void setVector(IntVector vector, Integer... values) {
       final int length = values.length;
       vector.allocateNew(length);
       for (int i = 0; i < length; i++) {
           if (values[i] != null) {
               vector.set(i, values[i]);
           }
       }
       vector.setValueCount(length);
   }

   void setVector(VarCharVector vector, byte[]... values) {
       final int length = values.length;
       vector.allocateNewSafe();
       for (int i = 0; i < length; i++) {
           if (values[i] != null) {
               vector.set(i, values[i]);
           }
       }
       vector.setValueCount(length);
   }

   void setVector(ListVector vector, List<Integer>... values) {
       vector.allocateNewSafe();
       Types.MinorType type = Types.MinorType.INT;
       vector.addOrGetVector(FieldType.nullable(type.getType()));

       IntVector dataVector = (IntVector) vector.getDataVector();
       dataVector.allocateNew();

       // set underlying vectors
       int curPos = 0;
       vector.getOffsetBuffer().setInt(0, curPos);
       for (int i = 0; i < values.length; i++) {
           if (values[i] == null) {
               BitVectorHelper.unsetBit(vector.getValidityBuffer(), i);
           } else {
               BitVectorHelper.setBit(vector.getValidityBuffer(), i);
               for (int value : values[i]) {
                   dataVector.setSafe(curPos, value);
                   curPos += 1;
               }
           }
           vector.getOffsetBuffer().setInt((i + 1) * BaseRepeatedValueVector.OFFSET_WIDTH, curPos);
       }
       dataVector.setValueCount(curPos);
       vector.setLastSet(values.length - 1);
       vector.setValueCount(values.length);
   }

   RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE); // deal with byte buffer allocation

Creating Vectors (arrays)
=========================

Array of Int (32-bit integer value)
-----------------------------------

.. code-block:: java
   :emphasize-lines: 4

   import org.apache.arrow.vector.IntVector;

   IntVector intVector = new IntVector("intVector", rootAllocator);
   setVector(intVector, 1,2,3);

.. code-block:: java
   :emphasize-lines: 1-3


   jshell> intVector

   intVector ==> [1, 2, 3]

Array of Varchar
----------------

.. code-block:: java
   :emphasize-lines: 4

   import org.apache.arrow.vector.VarCharVector;

   VarCharVector varcharVector = new VarCharVector("varcharVector", rootAllocator);
   setVector(varcharVector, "david".getBytes(), "gladis".getBytes(), "juan".getBytes());

.. code-block:: java
   :emphasize-lines: 1-3

   jshell> varcharVector

   varcharVector ==> [david, gladis, juan]

Array of List
-------------

.. code-block:: java
   :emphasize-lines: 6

   import org.apache.arrow.vector.complex.ListVector;

   import static java.util.Arrays.asList;

   ListVector listVector = ListVector.empty("listVector", rootAllocator);
   setVector(listVector, asList(1,3,5,7,9), asList(2,4,6,8,10), asList(1,2,3,5,8));

.. code-block:: java
   :emphasize-lines: 1-3

   jshell> listVector

   listVector ==> [[1,3,5,7,9], [2,4,6,8,10], [1,2,3,5,8]]

Creating VectorSchemaRoot (Table)
=================================

A `VectorSchemaRoot <https://arrow.apache.org/docs/java/vector_schema_root.html>`_ is a container that can hold batches, batches flow through VectorSchemaRoot as part of a pipeline.

.. code-block:: java
   :emphasize-lines: 21

   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.BitVectorHelper;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.vector.VarCharVector;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
   import org.apache.arrow.vector.complex.ListVector;
   import org.apache.arrow.vector.types.Types;
   import org.apache.arrow.vector.types.pojo.ArrowType;
   import org.apache.arrow.vector.types.pojo.Field;
   import org.apache.arrow.vector.types.pojo.FieldType;
   import org.apache.arrow.vector.types.pojo.Schema;

   import java.util.ArrayList;
   import java.util.HashMap;
   import java.util.List;
   import java.util.Map;

   import static java.util.Arrays.asList;

   VectorSchemaRoot createVectorSchemaRoot(){
       // create a column data type
       Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);

       Map<String, String> metadata = new HashMap<>();
       metadata.put("A", "Id card");
       metadata.put("B", "Passport");
       metadata.put("C", "Visa");
       Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), null, metadata), null);

       Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

       FieldType intType = new FieldType(true, new ArrowType.Int(32, true), /*dictionary=*/null);
       FieldType listType = new FieldType(true, new ArrowType.List(), /*dictionary=*/null);
       Field childField = new Field("intCol", intType, null);
       List<Field> childFields = new ArrayList<>();
       childFields.add(childField);
       Field points = new Field("points", listType, childFields);

       // create a definition
       Schema schemaPerson = new Schema(asList(name, document, age, points));

       RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE); // deal with byte buffer allocation
       VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);

       // getting field vectors
       VarCharVector nameVectorOption1 = (VarCharVector) vectorSchemaRoot.getVector("name"); //interface FieldVector
       VarCharVector documentVectorOption1 = (VarCharVector) vectorSchemaRoot.getVector("document"); //interface FieldVector
       IntVector ageVectorOption1 = (IntVector) vectorSchemaRoot.getVector("age");
       ListVector pointsVectorOption1 = (ListVector) vectorSchemaRoot.getVector("points");

       // add values to the field vectors
       setVector(nameVectorOption1, "david".getBytes(), "gladis".getBytes(), "juan".getBytes());
       setVector(documentVectorOption1, "A".getBytes(), "B".getBytes(), "C".getBytes());
       setVector(ageVectorOption1, 10,20,30);
       setVector(pointsVectorOption1, asList(1,3,5,7,9), asList(2,4,6,8,10), asList(1,2,3,5,8));
       vectorSchemaRoot.setRowCount(3);

       return vectorSchemaRoot;
   }

Lets create one VectorSchemaRoot (Table) using createVectorSchemaRoot method:

.. code-block:: 
   :emphasize-lines: 1-10

    jshell> VectorSchemaRoot vectorSchemaRoot = createVectorSchemaRoot();

    vectorSchemaRoot ==> org.apache.arrow.vector.VectorSchemaRoot@3d1848cc

    jshell> System.out.println(vectorSchemaRoot.contentToTSVString())

    name     document age   points
    david    A        10    [1,3,5,7,9]
    gladis   B        20    [2,4,6,8,10]
    juan     C        30    [1,2,3,5,8]

