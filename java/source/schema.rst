.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

===================
Working with Schema
===================

Let's start talking about tabular data. Data often comes in the form of two-dimensional
sets of heterogeneous data (such as database tables, CSV files...). Arrow provides
several abstractions to handle such data conveniently and efficiently.

.. contents::

Creating Fields
===============

Fields are used to denote the particular columns of tabular data.

.. testcode::

   import org.apache.arrow.vector.types.pojo.ArrowType;
   import org.apache.arrow.vector.types.pojo.Field;
   import org.apache.arrow.vector.types.pojo.FieldType;

   Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
   System.out.print(name);

.. testoutput::

   name: Utf8

.. testcode::

   import org.apache.arrow.vector.types.pojo.ArrowType;
   import org.apache.arrow.vector.types.pojo.Field;
   import org.apache.arrow.vector.types.pojo.FieldType;

   Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
   System.out.print(age);

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

   System.out.print(points);

.. testoutput::

   points: List<intCol: Int(32, true)>

Creating the Schema
===================

A schema describes a sequence of columns in tabular data, and consists
of a list of fields.

.. testcode::

   import org.apache.arrow.vector.types.pojo.Schema;
   import org.apache.arrow.vector.types.pojo.ArrowType;
   import org.apache.arrow.vector.types.pojo.Field;
   import org.apache.arrow.vector.types.pojo.FieldType;
   import java.util.ArrayList;
   import java.util.List;
   import static java.util.Arrays.asList;

   Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
   Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), null), null);
   Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
   FieldType intType = new FieldType(true, new ArrowType.Int(32, true), /*dictionary=*/null);
   FieldType listType = new FieldType(true, new ArrowType.List(), /*dictionary=*/null);
   Field childField = new Field("intCol", intType, null);
   List<Field> childFields = new ArrayList<>();
   childFields.add(childField);
   Field points = new Field("points", listType, childFields);
   Schema schemaPerson = new Schema(asList(name, document, age, points));

   System.out.print(schemaPerson);

.. testoutput::

   Schema<name: Utf8, document: Utf8, age: Int(32, true), points: List<intCol: Int(32, true)>>

Adding Metadata to Fields and Schemas
=====================================

In case we need to add metadata to our Field we could use:

.. testcode::

   import org.apache.arrow.vector.types.pojo.ArrowType;
   import org.apache.arrow.vector.types.pojo.Field;
   import org.apache.arrow.vector.types.pojo.FieldType;

   Map<String, String> metadata = new HashMap<>();
   metadata.put("A", "Id card");
   metadata.put("B", "Passport");
   metadata.put("C", "Visa");
   Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), null, metadata), null);

   System.out.print(document.getMetadata());

.. testoutput::

   {A=Id card, B=Passport, C=Visa}

In case we need to add metadata to our Schema we could use:

.. testcode::

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
   Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), null), null);
   Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
   FieldType intType = new FieldType(true, new ArrowType.Int(32, true), /*dictionary=*/null);
   FieldType listType = new FieldType(true, new ArrowType.List(), /*dictionary=*/null);
   Field childField = new Field("intCol", intType, null);
   List<Field> childFields = new ArrayList<>();
   childFields.add(childField);
   Field points = new Field("points", listType, childFields);
   Map<String, String> metadataSchema = new HashMap<>();
   metadataSchema.put("Key-1", "Value-1");
   Schema schemaPerson = new Schema(asList(name, document, age, points), metadataSchema);

   System.out.print(schemaPerson);

.. testoutput::

   Schema<name: Utf8, document: Utf8, age: Int(32, true), points: List<intCol: Int(32, true)>>(metadata: {Key-1=Value-1})

Creating VectorSchemaRoot
=========================

``VectorSchemaRoot`` is somewhat analogous to tables and record batches in the
other Arrow implementations in that they all are 2D datasets, but the usage is different.

Let's populate a ``VectorSchemaRoot`` with a small batch of records:

.. testcode::

   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.VarCharVector;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.arrow.vector.complex.ListVector;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.vector.complex.impl.UnionListWriter;
   import org.apache.arrow.vector.types.pojo.Schema;
   import org.apache.arrow.vector.types.pojo.ArrowType;
   import org.apache.arrow.vector.types.pojo.Field;
   import org.apache.arrow.vector.types.pojo.FieldType;
   import java.util.ArrayList;
   import java.util.List;
   import static java.util.Arrays.asList;

   Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
   Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
   FieldType intType = new FieldType(true, new ArrowType.Int(32, true), null);
   FieldType listType = new FieldType(true, new ArrowType.List(), null);
   Field childField = new Field("intCol", intType, null);
   List<Field> childFields = new ArrayList<>();
   childFields.add(childField);
   Field points = new Field("points", listType, childFields);
   Schema schema = new Schema(asList(name, age, points));
   try(
       BufferAllocator allocator = new RootAllocator();
       VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)
   ){
       VarCharVector nameVector = (VarCharVector) root.getVector("name");
       nameVector.allocateNew(3);
       nameVector.set(0, "David".getBytes());
       nameVector.set(1, "Gladis".getBytes());
       nameVector.set(2, "Juan".getBytes());
       nameVector.setValueCount(3);
       IntVector ageVector = (IntVector) root.getVector("age");
       ageVector.allocateNew(3);
       ageVector.set(0, 10);
       ageVector.set(1, 20);
       ageVector.set(2, 30);
       ageVector.setValueCount(3);
       ListVector listVector = (ListVector) root.getVector("points");
       UnionListWriter listWriter = listVector.getWriter();
       int[] data = new int[] { 4, 8, 12, 10, 20, 30, 5, 10, 15 };
       int tmp_index = 0;
       for(int i = 0; i < 3; i++) {
           listWriter.setPosition(i);
           listWriter.startList();
           for(int j = 0; j < 3; j++) {
               listWriter.writeInt(data[tmp_index]);
               tmp_index = tmp_index + 1;
           }
           listWriter.setValueCount(2);
           listWriter.endList();
       }
       listVector.setValueCount(3);
       root.setRowCount(3);

       System.out.print(root.contentToTSVString());
   } catch (Exception e) {
       e.printStackTrace();
   }

.. testoutput::

   name    age    points
   David    10    [4,8,12]
   Gladis    20    [10,20,30]
   Juan    30    [5,10,15]