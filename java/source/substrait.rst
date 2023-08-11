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

Arrow can use `Substrait`_ to integrate with other languages.

.. contents::

Querying Datasets
=================

The Substrait support in Arrow combines :doc:`Dataset <dataset>` and
`substrait-java`_ to query datasets using `Acero`_ as a backend.

Acero currently supports:

- Reading Arrow, CSV, ORC, and Parquet files
- Filters
- Projections
- Joins
- Aggregates

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

    Plan queryTableNation() throws SqlParseException {
       String sql = "SELECT * FROM NATION WHERE N_NATIONKEY = 17";
       String nation = "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), " +
               "N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))";
       SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
       Plan plan = sqlToSubstrait.execute(sql, ImmutableList.of(nation));
       return plan;
    }

    void queryDatasetThruSubstraitPlanDefinition() {
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

It is also possible to query multiple datasets and join them based on some criteria.
For example, we can join the nation and customer tables from the TPC-H benchmark:

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

    Plan queryTableNationJoinCustomer() throws SqlParseException {
        String sql = "SELECT n.n_name, COUNT(*) AS NUMBER_CUSTOMER FROM NATION n JOIN CUSTOMER c " +
            "ON n.n_nationkey = c.c_nationkey WHERE n.n_nationkey = 17 " +
            "GROUP BY n.n_name";
        String nation = "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, " +
            "N_NAME CHAR(25), N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))";
        String customer = "CREATE TABLE CUSTOMER (C_CUSTKEY BIGINT NOT NULL, " +
            "C_NAME VARCHAR(25), C_ADDRESS VARCHAR(40), C_NATIONKEY BIGINT NOT NULL, " +
            "C_PHONE CHAR(15), C_ACCTBAL DECIMAL, C_MKTSEGMENT CHAR(10), " +
            "C_COMMENT VARCHAR(117) )";
        SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
        Plan plan = sqlToSubstrait.execute(sql,
            ImmutableList.of(nation, customer));
        return plan;
    }

    void queryTwoDatasetsThruSubstraitPlanDefinition() {
        String uriNation = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/tpch/nation.parquet";
        String uriCustomer = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/tpch/customer.parquet";
        ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
        try (
            BufferAllocator allocator = new RootAllocator();
            DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(),
                FileFormat.PARQUET, uriNation);
            Dataset dataset = datasetFactory.finish();
            Scanner scanner = dataset.newScan(options);
            ArrowReader readerNation = scanner.scanBatches();
            DatasetFactory datasetFactoryCustomer = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(),
                FileFormat.PARQUET, uriCustomer);
            Dataset datasetCustomer = datasetFactoryCustomer.finish();
            Scanner scannerCustomer = datasetCustomer.newScan(options);
            ArrowReader readerCustomer = scannerCustomer.scanBatches()
        ) {
            // map table to reader
            Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
            mapTableToArrowReader.put("NATION", readerNation);
            mapTableToArrowReader.put("CUSTOMER", readerCustomer);
            // get binary plan
            Plan plan = queryTableNationJoinCustomer();
            ByteBuffer substraitPlan = ByteBuffer.allocateDirect(
                plan.toByteArray().length);
            substraitPlan.put(plan.toByteArray());
            // run query
            try (ArrowReader arrowReader = new AceroSubstraitConsumer(
                allocator).runQuery(
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

    queryTwoDatasetsThruSubstraitPlanDefinition();

.. testoutput::

    N_NAME    NUMBER_CUSTOMER
    PERU    573

.. _`Substrait`: https://substrait.io/
.. _`substrait-java`: https://github.com/substrait-io/substrait-java
.. _`Acero`: https://arrow.apache.org/docs/cpp/streaming_execution.html