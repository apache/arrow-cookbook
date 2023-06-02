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

.. _arrow-substrait:

=========
Substrait
=========

Arrow Java is using `Substrait`_ to leverage their integrations using standard
specification to share messages between different layer y/o languages.

.. contents::

Query Datasets
==============

Arrow :doc:`Java Dataset <dataset>` offer capabilities to read tabular data.
For other side `Substrait Java`_ offer serialization Plan for Relational Algebra.
Arrow Java Substrait is combined both of them to enable Querying data using
`Acero`_ as a backend.

Current `Acero`_ supported operations are:
- Read
- Filter
- Project
- Join
- Aggregate

Here is an example of a Java program that queries a Parquet file:

.. testcode::

    import com.google.common.collect.ImmutableList;
    import io.substrait.isthmus.SqlToSubstrait;
    import io.substrait.proto.Plan;
    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.dataset.substrait.AceroSubstraitConsumer;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowReader;
    import org.apache.calcite.sql.parser.SqlParseException;

    import java.nio.ByteBuffer;
    import java.util.HashMap;
    import java.util.Map;

    static Plan queryTableNation() throws SqlParseException {
       String sql = "SELECT * FROM NATION WHERE N_NATIONKEY = 17";
       String nation = "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), " +
               "N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))";
       SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
       Plan plan = sqlToSubstrait.execute(sql, ImmutableList.of(nation));
       return plan;
    }

    static void queryDatasetThruSubstraitPlanDefinition() {
       String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/tpch/nation.parquet";
       ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
       try (
           BufferAllocator allocator = new RootAllocator();
           DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(),
                   FileFormat.PARQUET, uri);
           Dataset dataset = datasetFactory.finish();
           Scanner scanner = dataset.newScan(options);
           ArrowReader reader = scanner.scanBatches()
       ) {
           // map table to reader
           Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
           mapTableToArrowReader.put("NATION", reader);
           // get binary plan
           Plan plan = queryTableNation();
           ByteBuffer substraitPlan = ByteBuffer.allocateDirect(plan.toByteArray().length);
           substraitPlan.put(plan.toByteArray());
           // run query
           try (ArrowReader arrowReader = new AceroSubstraitConsumer(allocator).runQuery(
                   substraitPlan,
                   mapTableToArrowReader
           )) {
               while (arrowReader.loadNextBatch()) {
                   System.out.print(arrowReader.getVectorSchemaRoot().contentToTSVString());
               }
           }
       } catch (Exception e) {
           e.printStackTrace();
       }
    }

    queryDatasetThruSubstraitPlanDefinition();

.. testoutput::

    N_NATIONKEY    N_NAME    N_REGIONKEY    N_COMMENT
    17    PERU    1    platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun

.. _`Substrait`: https://substrait.io/
.. _`Substrait Java`: https://github.com/substrait-io/substrait-java
.. _`Acero`: https://arrow.apache.org/docs/cpp/streaming_execution.html