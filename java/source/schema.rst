===================
Working with Schema
===================

Common definition of table has an schema. Java arrow is columnar oriented and it also has an schema representation.
Consider that each name on the schema maps to a columns for a predefined data type


.. contents::

Define Data Type
================

Definition of columnar fields for string (name), integer (age) and array (points):

.. testcode::

    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;

    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    System.out.print(name)

.. testoutput::

    name: Utf8

.. testcode::

    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;

    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    System.out.print(age)

.. testoutput::

    age: Int(32, true)

.. testcode::

    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;

    FieldType intType = new FieldType(true, new ArrowType.Int(32, true), null);
    FieldType listType = new FieldType(true, new ArrowType.List(), null);
    Field childField = new Field("intCol", intType, null);
    List<Field> childFields = new ArrayList<>();
    childFields.add(childField);
    Field points = new Field("points", listType, childFields);

    System.out.print(points)

.. testoutput::

    points: List<intCol: Int(32, true)>

Define Metadata for Field
=========================

In case we need to add metadata to our definition we could use:

.. testcode::

    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;

    // create a column data type + metadata
    Map<String, String> metadata = new HashMap<>();
    metadata.put("A", "Id card");
    metadata.put("B", "Passport");
    metadata.put("C", "Visa");
    Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), null, metadata), null);

    System.out.print(document.getMetadata())

.. testoutput::

    {A=Id card, B=Passport, C=Visa}

Create the Schema
=================

A schema is a list of Fields, where each Field is defined by name and type.

.. testcode::

    import org.apache.arrow.vector.types.pojo.Schema;
    import static java.util.Arrays.asList;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;

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

    System.out.print(schemaPerson)

.. testoutput::

    Schema<name: Utf8, document: Utf8, age: Int(32, true), points: List<intCol: Int(32, true)>>

Populate Data
=============

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.BitVectorHelper;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
    import org.apache.arrow.vector.complex.ListVector;
    import org.apache.arrow.vector.types.Types.MinorType;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;

    import java.util.ArrayList;
    import java.util.HashMap;
    import java.util.List;
    import java.util.Map;

    import static java.util.Arrays.asList;

    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Map<String, String> metadata = new HashMap<>();
    metadata.put("A", "Id card");
    metadata.put("B", "Passport");
    metadata.put("C", "Visa");
    Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), null, metadata), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    FieldType intType = new FieldType(true, new ArrowType.Int(32, true), null);
    FieldType listType = new FieldType(true, new ArrowType.List(), null);
    Field childField = new Field("intCol", intType, null);
    List<Field> childFields = new ArrayList<>();
    childFields.add(childField);
    Field points = new Field("points", listType, childFields);

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    Schema schema = new Schema(asList(name, document, age, points));
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator);

    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    nameVector.allocateNew(3);
    nameVector.set(0, "David".getBytes());
    nameVector.set(1, "Gladis".getBytes());
    nameVector.set(2, "Juan".getBytes());
    nameVector.setValueCount(3);
    VarCharVector documentVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    documentVector.allocateNew(3);
    documentVector.set(0, "A".getBytes());
    documentVector.set(1, "B".getBytes());
    documentVector.set(2, "C".getBytes());
    documentVector.setValueCount(3);
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    ageVector.setValueCount(3);
    ListVector listVector = (ListVector) vectorSchemaRoot.getVector("points");
    listVector.allocateNew();
    MinorType type = MinorType.INT;
    listVector.addOrGetVector(FieldType.nullable(type.getType()));
    IntVector dataVector = (IntVector) listVector.getDataVector();
    dataVector.allocateNew();
    listVector.getOffsetBuffer().setInt(0, 0);
    BitVectorHelper.setBit(listVector.getValidityBuffer(), 0);
    dataVector.set(0, 1);
    dataVector.set(1, 2);
    dataVector.set(2, 3);
    listVector.getOffsetBuffer().setInt(1 * BaseRepeatedValueVector.OFFSET_WIDTH, 3);
    BitVectorHelper.setBit(listVector.getValidityBuffer(), 1);
    dataVector.set(3, 9);
    dataVector.set(4, 8);
    listVector.getOffsetBuffer().setInt(2 * BaseRepeatedValueVector.OFFSET_WIDTH, 5);
    BitVectorHelper.setBit(listVector.getValidityBuffer(), 2);
    dataVector.set(5, 10);
    dataVector.set(6, 20);
    dataVector.set(7, 30);
    listVector.getOffsetBuffer().setInt(3 * BaseRepeatedValueVector.OFFSET_WIDTH, 8);
    listVector.setLastSet(2);
    listVector.setValueCount(3);

    vectorSchemaRoot.setRowCount(3);

    System.out.print(vectorSchemaRoot.contentToTSVString());

.. testoutput::

    name    document    age    points
    A    null    10    [1,2,3]
    B    null    20    [9,8]
    C    null    30    [10,20,30]
