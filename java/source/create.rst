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

Dictionary-Encoded Array of Varchar
-----------------------------------

In some scenarios `dictionary-encoding`_ a column is useful to save memory.

.. testcode::

   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.FieldVector;
   import org.apache.arrow.vector.VarCharVector;
   import org.apache.arrow.vector.dictionary.Dictionary;
   import org.apache.arrow.vector.dictionary.DictionaryEncoder;
   import org.apache.arrow.vector.types.pojo.ArrowType;
   import org.apache.arrow.vector.types.pojo.DictionaryEncoding;

   import java.nio.charset.StandardCharsets;

   try (BufferAllocator root = new RootAllocator();
        VarCharVector countries = new VarCharVector("country-dict", root);
        VarCharVector appUserCountriesUnencoded = new VarCharVector("app-use-country-dict", root)
   ) {
       countries.allocateNew(10);
       countries.set(0, "Andorra".getBytes(StandardCharsets.UTF_8));
       countries.set(1, "Cuba".getBytes(StandardCharsets.UTF_8));
       countries.set(2, "Grecia".getBytes(StandardCharsets.UTF_8));
       countries.set(3, "Guinea".getBytes(StandardCharsets.UTF_8));
       countries.set(4, "Islandia".getBytes(StandardCharsets.UTF_8));
       countries.set(5, "Malta".getBytes(StandardCharsets.UTF_8));
       countries.set(6, "Tailandia".getBytes(StandardCharsets.UTF_8));
       countries.set(7, "Uganda".getBytes(StandardCharsets.UTF_8));
       countries.set(8, "Yemen".getBytes(StandardCharsets.UTF_8));
       countries.set(9, "Zambia".getBytes(StandardCharsets.UTF_8));
       countries.setValueCount(10);

       Dictionary countriesDictionary = new Dictionary(countries,
               new DictionaryEncoding(/*id=*/1L, /*ordered=*/false, /*indexType=*/new ArrowType.Int(8, true)));
       System.out.println("Dictionary: " + countriesDictionary);

       appUserCountriesUnencoded.allocateNew(5);
       appUserCountriesUnencoded.set(0, "Andorra".getBytes(StandardCharsets.UTF_8));
       appUserCountriesUnencoded.set(1, "Guinea".getBytes(StandardCharsets.UTF_8));
       appUserCountriesUnencoded.set(2, "Islandia".getBytes(StandardCharsets.UTF_8));
       appUserCountriesUnencoded.set(3, "Malta".getBytes(StandardCharsets.UTF_8));
       appUserCountriesUnencoded.set(4, "Uganda".getBytes(StandardCharsets.UTF_8));
       appUserCountriesUnencoded.setValueCount(5);
       System.out.println("Unencoded data: " + appUserCountriesUnencoded);

       try (FieldVector appUserCountriesDictionaryEncoded = (FieldVector) DictionaryEncoder
               .encode(appUserCountriesUnencoded, countriesDictionary)) {
           System.out.println("Dictionary-encoded data: " + appUserCountriesDictionaryEncoded);
       }
   }

.. testoutput::

   Dictionary: Dictionary DictionaryEncoding[id=1,ordered=false,indexType=Int(8, true)] [Andorra, Cuba, Grecia, Guinea, Islandia, Malta, Tailandia, Uganda, Yemen, Zambia]
   Unencoded data: [Andorra, Guinea, Islandia, Malta, Uganda]
   Dictionary-encoded data: [0, 3, 4, 5, 7]

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


Slicing
=======

Slicing provides a way of copying a range of rows between two vectors of the same type.

Slicing IntVector
-----------------

In this example, we copy a portion of the input IntVector to a new IntVector.

.. testcode::

   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.vector.util.TransferPair;

   try (BufferAllocator allocator = new RootAllocator();
       IntVector vector = new IntVector("intVector", allocator)) {
       for (int i = 0; i < 10; i++) {
           vector.setSafe(i, i);
        }
       vector.setValueCount(10);

       TransferPair tp = vector.getTransferPair(allocator);
       tp.splitAndTransfer(0, 5);
       try (IntVector sliced = (IntVector) tp.getTo()) {
           System.out.println(sliced);
       }
       
       tp = vector.getTransferPair(allocator);
       // copy 6 elements from index 2
       tp.splitAndTransfer(2, 6);
       try (IntVector sliced = (IntVector) tp.getTo()) {
           System.out.print(sliced);
       }
   }

.. testoutput::

   [0, 1, 2, 3, 4]
   [2, 3, 4, 5, 6, 7]
   
.. _`FieldVector`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/FieldVector.html
.. _`ValueVector`: https://arrow.apache.org/docs/java/vector.html
.. _`dictionary-encoding`: https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout
