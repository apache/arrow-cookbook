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

.. _arrow-avro:

======
Avro 
======

Avro encoded data can be converted into Arrow format.

.. contents::

Avro to Arrow
=============

The example assumes that the Avro schema is stored separately from the Avro data itself.

.. testcode::

   import org.apache.arrow.AvroToArrow;
   import org.apache.arrow.AvroToArrowConfig;
   import org.apache.arrow.AvroToArrowConfigBuilder;
   import org.apache.arrow.AvroToArrowVectorIterator;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.avro.Schema;
   import org.apache.avro.io.BinaryDecoder;
   import org.apache.avro.io.DecoderFactory;

   import java.io.File;
   import java.io.FileInputStream;
   import java.io.FileNotFoundException;
   import java.io.IOException;

   try {
       BinaryDecoder decoder = new DecoderFactory().binaryDecoder(new FileInputStream("./thirdpartydeps/avro/users.avro"), null);
       Schema schema = new Schema.Parser().parse(new File("./thirdpartydeps/avro/user.avsc"));
       try (BufferAllocator allocator = new RootAllocator()) {
           AvroToArrowConfig config = new AvroToArrowConfigBuilder(allocator).build();
           try (AvroToArrowVectorIterator avroToArrowVectorIterator = AvroToArrow.avroToArrowIterator(schema, decoder, config)) {
               while(avroToArrowVectorIterator.hasNext()) {
                   try (VectorSchemaRoot root = avroToArrowVectorIterator.next()) {
                       System.out.print(root.contentToTSVString());
                   }
               }
           }
       }
   } catch (Exception e) {
       e.printStackTrace();
   } 

.. testoutput::

   name    favorite_number    favorite_color
   Alyssa    256    null
   Ben    7    red
