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

.. _arrow-dataset:

=======
Dataset
=======

* `Arrow Java Dataset`_: Java implementation of Arrow Datasets library. Implement Dataset Java API by JNI to C++.

.. contents::

Constructing Datasets
=====================

We can construct a dataset with an auto-inferred schema.

.. testcode::

   import org.apache.arrow.dataset.file.FileFormat;
   import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
   import org.apache.arrow.dataset.jni.NativeMemoryPool;
   import org.apache.arrow.dataset.scanner.ScanOptions;
   import org.apache.arrow.dataset.scanner.Scanner;
   import org.apache.arrow.dataset.source.Dataset;
   import org.apache.arrow.dataset.source.DatasetFactory;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import java.util.stream.StreamSupport;

   String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
   ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
   try (
       BufferAllocator allocator = new RootAllocator();
       DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
       Dataset dataset = datasetFactory.finish();
       Scanner scanner = dataset.newScan(options)
   ) {
       System.out.println(StreamSupport.stream(scanner.scan().spliterator(), false).count());
   } catch (Exception e) {
       e.printStackTrace();
   }

.. testoutput::

   1

Let construct our dataset with predefined schema.

.. testcode::

   import org.apache.arrow.dataset.file.FileFormat;
   import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
   import org.apache.arrow.dataset.jni.NativeMemoryPool;
   import org.apache.arrow.dataset.scanner.ScanOptions;
   import org.apache.arrow.dataset.scanner.Scanner;
   import org.apache.arrow.dataset.source.Dataset;
   import org.apache.arrow.dataset.source.DatasetFactory;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import java.util.stream.StreamSupport;

   String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
   ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
   try (
       BufferAllocator allocator = new RootAllocator();
       DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
       Dataset dataset = datasetFactory.finish(datasetFactory.inspect());
       Scanner scanner = dataset.newScan(options)
   ) {
       System.out.println(StreamSupport.stream(scanner.scan().spliterator(), false).count());
   } catch (Exception e) {
       e.printStackTrace();
   }

.. testoutput::

   1

Getting the Schema
==================

During Dataset Construction
***************************

.. testcode::

   import org.apache.arrow.dataset.file.FileFormat;
   import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
   import org.apache.arrow.dataset.jni.NativeMemoryPool;
   import org.apache.arrow.dataset.source.DatasetFactory;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.types.pojo.Schema;

   String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
   try (
       BufferAllocator allocator = new RootAllocator();
       DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri)
   ) {
       Schema schema = datasetFactory.inspect();

       System.out.println(schema);
   } catch (Exception e) {
       e.printStackTrace();
   }

.. testoutput::

   Schema<id: Int(32, true), name: Utf8>(metadata: {parquet.avro.schema={"type":"record","name":"User","namespace":"org.apache.arrow.dataset","fields":[{"name":"id","type":["int","null"]},{"name":"name","type":["string","null"]}]}, writer.model.name=avro})

From a Dataset
**************

.. testcode::

   import org.apache.arrow.dataset.file.FileFormat;
   import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
   import org.apache.arrow.dataset.jni.NativeMemoryPool;
   import org.apache.arrow.dataset.scanner.ScanOptions;
   import org.apache.arrow.dataset.scanner.Scanner;
   import org.apache.arrow.dataset.source.Dataset;
   import org.apache.arrow.dataset.source.DatasetFactory;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.types.pojo.Schema;

   String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
   ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
   try (
       BufferAllocator allocator = new RootAllocator();
       DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
       Dataset dataset = datasetFactory.finish();
       Scanner scanner = dataset.newScan(options)
   ) {
       Schema schema = scanner.schema();

       System.out.println(schema);
   } catch (Exception e) {
       e.printStackTrace();
   }

.. testoutput::

   Schema<id: Int(32, true), name: Utf8>(metadata: {parquet.avro.schema={"type":"record","name":"User","namespace":"org.apache.arrow.dataset","fields":[{"name":"id","type":["int","null"]},{"name":"name","type":["string","null"]}]}, writer.model.name=avro})

Query Parquet File
==================

Let query information for a parquet file.

Query Data Content For File
***************************

.. testcode::

   import org.apache.arrow.dataset.file.FileFormat;
   import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
   import org.apache.arrow.dataset.jni.NativeMemoryPool;
   import org.apache.arrow.dataset.scanner.ScanOptions;
   import org.apache.arrow.dataset.scanner.Scanner;
   import org.apache.arrow.dataset.source.Dataset;
   import org.apache.arrow.dataset.source.DatasetFactory;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.arrow.vector.ipc.ArrowReader;

   String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
   ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
   try (
       BufferAllocator allocator = new RootAllocator();
       DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
       Dataset dataset = datasetFactory.finish();
       Scanner scanner = dataset.newScan(options);
       ArrowReader reader = scanner.scanBatches()
   ) {
       while (reader.loadNextBatch()) {
           try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
               System.out.print(root.contentToTSVString());
           }
       }
   } catch (Exception e) {
       e.printStackTrace();
   }

.. testoutput::

   id    name
   1    David
   2    Gladis
   3    Juan

Let's try to read a Parquet file with gzip compression and 3 row groups:

.. code-block::

   $ parquet-tools meta data4_3rg_gzip.parquet

   file schema: schema
   age:         OPTIONAL INT64 R:0 D:1
   name:        OPTIONAL BINARY L:STRING R:0 D:1
   row group 1: RC:4 TS:182 OFFSET:4
   row group 2: RC:4 TS:190 OFFSET:420
   row group 3: RC:3 TS:179 OFFSET:838

.. testcode::

   import org.apache.arrow.dataset.file.FileFormat;
   import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
   import org.apache.arrow.dataset.jni.NativeMemoryPool;
   import org.apache.arrow.dataset.scanner.ScanOptions;
   import org.apache.arrow.dataset.scanner.Scanner;
   import org.apache.arrow.dataset.source.Dataset;
   import org.apache.arrow.dataset.source.DatasetFactory;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.arrow.vector.ipc.ArrowReader;

   String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data4_3rg_gzip.parquet";
   ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
   try (
       BufferAllocator allocator = new RootAllocator();
       DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
       Dataset dataset = datasetFactory.finish();
       Scanner scanner = dataset.newScan(options);
       ArrowReader reader = scanner.scanBatches()
   ) {
       int totalBatchSize = 0;
       int count = 1;
       while (reader.loadNextBatch()) {
           try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
               totalBatchSize += root.getRowCount();
               System.out.println("Number of rows per batch["+ count++ +"]: " + root.getRowCount());
               System.out.print(root.contentToTSVString());
           }
       }
       System.out.println("Total batch size: " + totalBatchSize);
   } catch (Exception e) {
       e.printStackTrace();
   }

.. testoutput::

   Number of rows per batch[1]: 4
   age    name
   10    Jean
   10    Lu
   10    Kei
   10    Sophia
   Number of rows per batch[2]: 4
   age    name
   10    Mara
   20    Arit
   20    Neil
   20    Jason
   Number of rows per batch[3]: 3
   age    name
   20    John
   20    Peter
   20    Ismael
   Total batch size: 11

Query Data Content For Directory
********************************

Consider that we have these files: data1: 3 rows, data2: 3 rows and data3: 250 rows.

.. testcode::

   import org.apache.arrow.dataset.file.FileFormat;
   import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
   import org.apache.arrow.dataset.jni.NativeMemoryPool;
   import org.apache.arrow.dataset.scanner.ScanOptions;
   import org.apache.arrow.dataset.scanner.Scanner;
   import org.apache.arrow.dataset.source.Dataset;
   import org.apache.arrow.dataset.source.DatasetFactory;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.arrow.vector.ipc.ArrowReader;

   String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/";
   ScanOptions options = new ScanOptions(/*batchSize*/ 100);
   try (
       BufferAllocator allocator = new RootAllocator();
       DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
       Dataset dataset = datasetFactory.finish();
       Scanner scanner = dataset.newScan(options);
       ArrowReader reader = scanner.scanBatches()
   ) {
       int count = 1;
       while (reader.loadNextBatch()) {
           try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
               System.out.println("Batch: " + count++ + ", RowCount: " + root.getRowCount());
           }
       }
   } catch (Exception e) {
       e.printStackTrace();
   }

.. testoutput::

   Batch: 1, RowCount: 3
   Batch: 2, RowCount: 3
   Batch: 3, RowCount: 100
   Batch: 4, RowCount: 100
   Batch: 5, RowCount: 50
   Batch: 6, RowCount: 4
   Batch: 7, RowCount: 4
   Batch: 8, RowCount: 3

Query Data Content with Projection
**********************************

In case we need to project only certain columns we could configure ScanOptions with projections needed.

.. testcode::

   import org.apache.arrow.dataset.file.FileFormat;
   import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
   import org.apache.arrow.dataset.jni.NativeMemoryPool;
   import org.apache.arrow.dataset.scanner.ScanOptions;
   import org.apache.arrow.dataset.scanner.Scanner;
   import org.apache.arrow.dataset.source.Dataset;
   import org.apache.arrow.dataset.source.DatasetFactory;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.arrow.vector.ipc.ArrowReader;

   String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
   String[] projection = new String[] {"name"};
   ScanOptions options = new ScanOptions(/*batchSize*/ 32768, Optional.of(projection));
   try (
       BufferAllocator allocator = new RootAllocator();
       DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
       Dataset dataset = datasetFactory.finish();
       Scanner scanner = dataset.newScan(options);
       ArrowReader reader = scanner.scanBatches()
   ) {
       while (reader.loadNextBatch()) {
           try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
               System.out.print(root.contentToTSVString());
           }
       }
   } catch (Exception e) {
       e.printStackTrace();
   }

.. testoutput::

   name
   David
   Gladis
   Juan

Query Arrow Files
=================


Query Data Content For File
***************************

Let's read an Arrow file with 3 record batches, each with 3 rows.

.. testcode::

   import org.apache.arrow.dataset.file.FileFormat;
   import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
   import org.apache.arrow.dataset.jni.NativeMemoryPool;
   import org.apache.arrow.dataset.scanner.ScanOptions;
   import org.apache.arrow.dataset.scanner.Scanner;
   import org.apache.arrow.dataset.source.Dataset;
   import org.apache.arrow.dataset.source.DatasetFactory;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.arrow.vector.ipc.ArrowReader;

   import java.io.IOException;

   String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/arrowfiles/random_access.arrow";
   ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
   try (
       BufferAllocator allocator = new RootAllocator();
       DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.ARROW_IPC, uri);
       Dataset dataset = datasetFactory.finish();
       Scanner scanner = dataset.newScan(options);
       ArrowReader reader = scanner.scanBatches()
   ) {
       int count = 1;
       while (reader.loadNextBatch()) {
           try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
               System.out.println("Number of rows per batch["+ count++ +"]: " + root.getRowCount());
           }
       }
   } catch (Exception e) {
       e.printStackTrace();
   }

.. testoutput::

   Number of rows per batch[1]: 3
   Number of rows per batch[2]: 3
   Number of rows per batch[3]: 3

Query ORC File
==============

Query Data Content For File
***************************

Let's read an ORC file with zlib compression 385 stripes, each with 5000 rows.

.. code-block::

   $ orc-metadata demo-11-zlib.orc | more

   { "name": "demo-11-zlib.orc",
     "type": "struct<_col0:int,_col1:string,_col2:string,_col3:string,_col4:int,_col5:string,_col6:int,_col7:int,_col8:int>",
     "stripe count": 385,
     "compression": "zlib", "compression block": 262144,
     "stripes": [
       { "stripe": 0, "rows": 5000,
         "offset": 3, "length": 1031,
         "index": 266, "data": 636, "footer": 129
       },
   ...

.. testcode::

   import org.apache.arrow.dataset.file.FileFormat;
   import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
   import org.apache.arrow.dataset.jni.NativeMemoryPool;
   import org.apache.arrow.dataset.scanner.ScanOptions;
   import org.apache.arrow.dataset.scanner.Scanner;
   import org.apache.arrow.dataset.source.Dataset;
   import org.apache.arrow.dataset.source.DatasetFactory;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.arrow.vector.ipc.ArrowReader;

   String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/orc/data1-zlib.orc";
   ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
   try (
       BufferAllocator allocator = new RootAllocator();
       DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.ORC, uri);
       Dataset dataset = datasetFactory.finish();
       Scanner scanner = dataset.newScan(options);
       ArrowReader reader = scanner.scanBatches()
   ) {
       int totalBatchSize = 0;
       while (reader.loadNextBatch()) {
           try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
               totalBatchSize += root.getRowCount();
           }
       }
       System.out.println("Total batch size: " + totalBatchSize);
   } catch (Exception e) {
       e.printStackTrace();
   }

.. testoutput::

   Total batch size: 1920800

Query CSV File
==============

Query Data Content For File
***************************

Let's read a CSV file.

.. testcode::

   import org.apache.arrow.dataset.file.FileFormat;
   import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
   import org.apache.arrow.dataset.jni.NativeMemoryPool;
   import org.apache.arrow.dataset.scanner.ScanOptions;
   import org.apache.arrow.dataset.scanner.Scanner;
   import org.apache.arrow.dataset.source.Dataset;
   import org.apache.arrow.dataset.source.DatasetFactory;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.arrow.vector.ipc.ArrowReader;

   String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/csv/tech_acquisitions.csv";
   ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
   try (
       BufferAllocator allocator = new RootAllocator();
       DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.CSV, uri);
       Dataset dataset = datasetFactory.finish();
       Scanner scanner = dataset.newScan(options);
       ArrowReader reader = scanner.scanBatches()
   ) {
       int totalBatchSize = 0;
       while (reader.loadNextBatch()) {
           try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
               totalBatchSize += root.getRowCount();
               System.out.print(root.contentToTSVString());
           }
       }
       System.out.println("Total batch size: " + totalBatchSize);
   } catch (Exception e) {
       e.printStackTrace();
   }

.. testoutput::

   Acquirer    Acquiree    Amount in billions (USD)    Date of acquisition
   NVIDIA    Mellanox    6.9    04/05/2020
   AMD    Xilinx    35.0    27/10/2020
   Salesforce    Slack    27.7    01/12/2020
   Total batch size: 3

.. _Arrow Java Dataset: https://arrow.apache.org/docs/dev/java/dataset.html