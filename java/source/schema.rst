===================
Working with schema
===================

Common definition of table has an schema. Java arrow is columnar oriented and it also has an schema representation. 
Consider that each name on the schema maps to a columns for a predefined data type

.. contents::

Define data type
================

Definition of columnar fields for string (name), integer (age) and array (points):

.. code-block:: java
   :emphasize-lines: 6,8,12,15

   import org.apache.arrow.vector.types.pojo.ArrowType;
   import org.apache.arrow.vector.types.pojo.Field;
   import org.apache.arrow.vector.types.pojo.FieldType;

   // create a column data type
   Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);

   Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

   FieldType intType = new FieldType(true, new ArrowType.Int(32, true), /*dictionary=*/null);
   FieldType listType = new FieldType(true, new ArrowType.List(), /*dictionary=*/null);
   Field childField = new Field("intCol", intType, null);
   List<Field> childFields = new ArrayList<>();
   childFields.add(childField);
   Field points = new Field("points", listType, childFields);

.. code-block:: java
   :emphasize-lines: 1-5

   jshell> name; age; points;

   name ==> name: Utf8
   age ==> age: Int(32, true)
   points ==> points: List<intCol: Int(32, true)>

Define metadata
===============

In case we need to add metadata to our definition we could use:

.. code-block:: java
   :emphasize-lines: 10

   import org.apache.arrow.vector.types.pojo.ArrowType;
   import org.apache.arrow.vector.types.pojo.Field;
   import org.apache.arrow.vector.types.pojo.FieldType;

   // create a column data type + metadata
   Map<String, String> metadata = new HashMap<>();
   metadata.put("A", "Id card");
   metadata.put("B", "Passport");
   metadata.put("C", "Visa");
   Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), null, metadata), null);

.. code-block:: java
   :emphasize-lines: 1-3

   jshell> document

   document ==> document: Utf8

Create the schema
=================

A schema is a list of Fields, where each Field is defined by name and type.

.. code-block:: java
   :emphasize-lines: 5

   import org.apache.arrow.vector.types.pojo.Schema;
   import static java.util.Arrays.asList;

   // create a definition
   Schema schemaPerson = new Schema(asList(name, document, age, points));

.. code-block:: java
   :emphasize-lines: 1-3

   jshell> schemaPerson

   schemaPerson ==> Schema<name: Utf8, document: Utf8, age: Int(32, true), points: List<intCol: Int(32, true)>>

Populate data
=============

.. code-block:: java
   :emphasize-lines: 12,23,34

   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.BitVectorHelper;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.vector.VarCharVector;
   import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
   import org.apache.arrow.vector.complex.ListVector;
   import org.apache.arrow.vector.types.Types;
   import org.apache.arrow.vector.types.pojo.FieldType;
   import org.apache.arrow.vector.VectorSchemaRoot;

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

   RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

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

Render data & metadata:

.. code-block:: java
   :emphasize-lines: 1,8

   jshell> System.out.println(vectorSchemaRoot.contentToTSVString());

   name    document    age  points
   david   A            10  [1,3,5,7,9]
   gladis  B            20  [2,4,6,8,10]
   juan    C            30  [1,2,3,5,8]

   jshell> System.out.println(documentVectorOption1.getField().getMetadata());

   {A=Id card, B=Passport, C=Visa}

Create the schema from json
===========================

For this json definition:

.. code-block:: java

    jshell> System.out.println(schemaPerson.toJson());

    {
      "fields" : [ {
        "name" : "name",
        "nullable" : true,
        "type" : {
          "name" : "utf8"
        },
        "children" : [ ]
      }, {
        "name" : "document",
        "nullable" : true,
        "type" : {
          "name" : "utf8"
        },
        "children" : [ ],
        "metadata" : [ {
          "value" : "Id card",
          "key" : "A"
        }, {
          "value" : "Passport",
          "key" : "B"
        }, {
          "value" : "Visa",
          "key" : "C"
        } ]
      }, {
        "name" : "age",
        "nullable" : true,
        "type" : {
          "name" : "int",
          "bitWidth" : 32,
          "isSigned" : true
        },
        "children" : [ ]
      }, {
        "name" : "points",
        "nullable" : true,
        "type" : {
          "name" : "list"
        },
        "children" : [ {
          "name" : "intCol",
          "nullable" : true,
          "type" : {
            "name" : "int",
            "bitWidth" : 32,
            "isSigned" : true
          },
          "children" : [ ]
        } ]
      } ]
    }

Java arrow offer Schema.fromJSON() method to create an schema from json definition

.. code-block:: java
   :emphasize-lines: 3

   // create an schema from json
   String jsonSchemaDefnition = schemaPerson.toJson();
   Schema schemaPersonFromJson = Schema.fromJSON(jsonSchemaDefnition);

.. code-block:: java
   :emphasize-lines: 1,5

    jshell> schemaPersonFromJson

    schemaPersonFromJson ==> Schema<name: Utf8, document: Utf8, age: Int(32, true), points: List<intCol: Int(32, true)>>

    jshell> System.out.println(schemaPersonFromJson.toJson());
    {
      "fields" : [ {
        "name" : "name",
        "nullable" : true,
        "type" : {
          "name" : "utf8"
        },
        "children" : [ ]
      }, {
        "name" : "document",
        "nullable" : true,
        "type" : {
          "name" : "utf8"
        },
        "children" : [ ],
        "metadata" : [ {
          "value" : "Id card",
          "key" : "A"
        }, {
          "value" : "Passport",
          "key" : "B"
        }, {
          "value" : "Visa",
          "key" : "C"
        } ]
      }, {
        "name" : "age",
        "nullable" : true,
        "type" : {
          "name" : "int",
          "bitWidth" : 32,
          "isSigned" : true
        },
        "children" : [ ]
      }, {
        "name" : "points",
        "nullable" : true,
        "type" : {
          "name" : "list"
        },
        "children" : [ {
          "name" : "intCol",
          "nullable" : true,
          "type" : {
            "name" : "int",
            "bitWidth" : 32,
            "isSigned" : true
          },
          "children" : [ ]
        } ]
      } ]
    }


