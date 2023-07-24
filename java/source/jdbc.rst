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

.. _arrow-jdbc:

==================
Arrow JDBC Adapter
==================

The `Arrow Java JDBC module <https://arrow.apache.org/docs/java/jdbc.html>`_
converts JDBC ResultSets into Arrow VectorSchemaRoots.

.. contents::

ResultSet to VectorSchemaRoot Conversion
========================================

The main class to help us to convert ResultSet to VectorSchemaRoot is
`JdbcToArrow <https://arrow.apache.org/docs/java/reference/org/apache/arrow/adapter/jdbc/JdbcToArrow.html>`_

.. testcode::

   import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
   import org.apache.arrow.adapter.jdbc.JdbcToArrow;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.ibatis.jdbc.ScriptRunner;

   import java.io.BufferedReader;
   import java.io.FileReader;
   import java.io.IOException;
   import java.sql.Connection;
   import java.sql.DriverManager;
   import java.sql.ResultSet;
   import java.sql.SQLException;

   try (BufferAllocator allocator = new RootAllocator();
        Connection connection = DriverManager.getConnection(
                "jdbc:h2:mem:h2-jdbc-adapter")) {
       ScriptRunner runnerDDLDML = new ScriptRunner(connection);
       runnerDDLDML.setLogWriter(null);
       runnerDDLDML.runScript(new BufferedReader(
               new FileReader("./thirdpartydeps/jdbc/h2-ddl.sql")));
       runnerDDLDML.runScript(new BufferedReader(
               new FileReader("./thirdpartydeps/jdbc/h2-dml.sql")));
       try (ResultSet resultSet = connection.createStatement().executeQuery(
               "SELECT int_field1, bool_field2, bigint_field5 FROM TABLE1");
            ArrowVectorIterator iterator = JdbcToArrow.sqlToArrowVectorIterator(
                    resultSet, allocator)) {
           while (iterator.hasNext()) {
               try (VectorSchemaRoot root = iterator.next()) {
                   System.out.print(root.contentToTSVString());
               }
           }
       }
   } catch (SQLException | IOException e) {
       e.printStackTrace();
   }

.. testoutput::

   INT_FIELD1    BOOL_FIELD2    BIGINT_FIELD5
   101    true    1000000000300
   102    true    100000000030
   103    true    10000000003

Configuring Array subtypes
==========================

JdbcToArrow accepts configuration through `JdbcToArrowConfig
<https://arrow.apache.org/docs/java/reference/org/apache/arrow/adapter/jdbc/JdbcToArrowConfig.html>`_.
For example, the type of the elements of array columns can be specified by
``setArraySubTypeByColumnNameMap``.

.. testcode::

   import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
   import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
   import org.apache.arrow.adapter.jdbc.JdbcToArrow;
   import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
   import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
   import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.ibatis.jdbc.ScriptRunner;

   import java.io.BufferedReader;
   import java.io.FileReader;
   import java.io.IOException;
   import java.sql.Connection;
   import java.sql.DriverManager;
   import java.sql.ResultSet;
   import java.sql.SQLException;
   import java.sql.Types;
   import java.util.HashMap;

   try (BufferAllocator allocator = new RootAllocator();
        Connection connection = DriverManager.getConnection(
                "jdbc:h2:mem:h2-jdbc-adapter")) {
       ScriptRunner runnerDDLDML = new ScriptRunner(connection);
       runnerDDLDML.setLogWriter(null);
       runnerDDLDML.runScript(new BufferedReader(
               new FileReader("./thirdpartydeps/jdbc/h2-ddl.sql")));
       runnerDDLDML.runScript(new BufferedReader(
               new FileReader("./thirdpartydeps/jdbc/h2-dml.sql")));
       JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(allocator,
               JdbcToArrowUtils.getUtcCalendar())
               .setArraySubTypeByColumnNameMap(
                       new HashMap<>() {{
                           put("LIST_FIELD19",
                                   new JdbcFieldInfo(Types.INTEGER));
                       }}
               )
               .build();
       try (ResultSet resultSet = connection.createStatement().executeQuery(
               "SELECT int_field1, bool_field2, bigint_field5, char_field16, list_field19 FROM TABLE1");
            ArrowVectorIterator iterator = JdbcToArrow.sqlToArrowVectorIterator(
                    resultSet, config)) {
           while (iterator.hasNext()) {
               try (VectorSchemaRoot root = iterator.next()) {
                   System.out.print(root.contentToTSVString());
               }
           }
       }
   } catch (SQLException | IOException e) {
       e.printStackTrace();
   }

.. testoutput::

   INT_FIELD1    BOOL_FIELD2    BIGINT_FIELD5    CHAR_FIELD16    LIST_FIELD19
   101    true    1000000000300    some char text      [1,2,3]
   102    true    100000000030    some char text      [1,2]
   103    true    10000000003    some char text      [1]

Configuring batch size
======================

By default, the adapter will read up to 1024 rows in a batch. This
can be customized via ``setTargetBatchSize``.

.. testcode::

   import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
   import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
   import org.apache.arrow.adapter.jdbc.JdbcToArrow;
   import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
   import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
   import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.ibatis.jdbc.ScriptRunner;

   import java.io.BufferedReader;
   import java.io.FileReader;
   import java.io.IOException;
   import java.sql.Connection;
   import java.sql.DriverManager;
   import java.sql.ResultSet;
   import java.sql.SQLException;
   import java.sql.Types;
   import java.util.HashMap;

   try (BufferAllocator allocator = new RootAllocator();
        Connection connection = DriverManager.getConnection(
                "jdbc:h2:mem:h2-jdbc-adapter")) {
       ScriptRunner runnerDDLDML = new ScriptRunner(connection);
       runnerDDLDML.setLogWriter(null);
       runnerDDLDML.runScript(new BufferedReader(
               new FileReader("./thirdpartydeps/jdbc/h2-ddl.sql")));
       runnerDDLDML.runScript(new BufferedReader(
               new FileReader("./thirdpartydeps/jdbc/h2-dml.sql")));
       JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(allocator,
               JdbcToArrowUtils.getUtcCalendar())
               .setTargetBatchSize(2)
               .setArraySubTypeByColumnNameMap(
                       new HashMap<>() {{
                           put("LIST_FIELD19",
                                   new JdbcFieldInfo(Types.INTEGER));
                       }}
               )
               .build();
       try (ResultSet resultSet = connection.createStatement().executeQuery(
               "SELECT int_field1, bool_field2, bigint_field5, char_field16, list_field19 FROM TABLE1");
            ArrowVectorIterator iterator = JdbcToArrow.sqlToArrowVectorIterator(
                    resultSet, config)) {
           while (iterator.hasNext()) {
               try (VectorSchemaRoot root = iterator.next()) {
                   System.out.print(root.contentToTSVString());
               }
           }
       }
   } catch (SQLException | IOException e) {
       e.printStackTrace();
   }

.. testoutput::

   INT_FIELD1    BOOL_FIELD2    BIGINT_FIELD5    CHAR_FIELD16    LIST_FIELD19
   101    true    1000000000300    some char text      [1,2,3]
   102    true    100000000030    some char text      [1,2]
   INT_FIELD1    BOOL_FIELD2    BIGINT_FIELD5    CHAR_FIELD16    LIST_FIELD19
   103    true    10000000003    some char text      [1]

Configuring numeric (decimal) precision and scale
=================================================

By default, the scale of any decimal values must exactly match the defined
scale of the Arrow type of the column, or else an UnsupportedOperationException
will be thrown with a message like ``BigDecimal scale must equal that in the Arrow
vector``.

This can happen because Arrow infers the type from the ResultSet metadata, which
is not accurate for all database drivers. The JDBC adapter lets you avoid this
by either overriding the decimal scale, or by providing a RoundingMode via
``setBigDecimalRoundingMode`` to convert values to the expected scale.

In this example, we have a BigInt column. By default, the inferred scale
is 0. We override the scale to 7 and then set a RoundingMode to convert
values to the given scale.

.. testcode::

   import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
   import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
   import org.apache.arrow.adapter.jdbc.JdbcToArrow;
   import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
   import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
   import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.ibatis.jdbc.ScriptRunner;

   import java.io.BufferedReader;
   import java.io.FileReader;
   import java.io.IOException;
   import java.math.RoundingMode;
   import java.sql.Connection;
   import java.sql.DriverManager;
   import java.sql.ResultSet;
   import java.sql.SQLException;
   import java.sql.Types;
   import java.util.HashMap;

   try (BufferAllocator allocator = new RootAllocator();
        Connection connection = DriverManager.getConnection(
                "jdbc:h2:mem:h2-jdbc-adapter")) {
       ScriptRunner runnerDDLDML = new ScriptRunner(connection);
       runnerDDLDML.setLogWriter(null);
       runnerDDLDML.runScript(new BufferedReader(
               new FileReader("./thirdpartydeps/jdbc/h2-ddl.sql")));
       runnerDDLDML.runScript(new BufferedReader(
               new FileReader("./thirdpartydeps/jdbc/h2-dml.sql")));
       JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(allocator,
               JdbcToArrowUtils.getUtcCalendar())
               .setTargetBatchSize(2)
               .setArraySubTypeByColumnNameMap(
                       new HashMap<>() {{
                           put("LIST_FIELD19",
                                   new JdbcFieldInfo(Types.INTEGER));
                       }}
               )
               .setExplicitTypesByColumnName(
                       new HashMap<>() {{
                           put("BIGINT_FIELD5",
                                   new JdbcFieldInfo(Types.DECIMAL, 20, 7));
                       }}
               )
               .setBigDecimalRoundingMode(RoundingMode.UNNECESSARY)
               .build();
       try (ResultSet resultSet = connection.createStatement().executeQuery(
               "SELECT int_field1, bool_field2, bigint_field5, char_field16, list_field19 FROM TABLE1");
            ArrowVectorIterator iterator = JdbcToArrow.sqlToArrowVectorIterator(
                    resultSet, config)) {
           while (iterator.hasNext()) {
               try (VectorSchemaRoot root = iterator.next()) {
                   System.out.print(root.contentToTSVString());
               }
           }
       }
   } catch (SQLException | IOException e) {
       e.printStackTrace();
   }

.. testoutput::

   INT_FIELD1    BOOL_FIELD2    BIGINT_FIELD5    CHAR_FIELD16    LIST_FIELD19
   101    true    1000000000300.0000000    some char text      [1,2,3]
   102    true    100000000030.0000000    some char text      [1,2]
   INT_FIELD1    BOOL_FIELD2    BIGINT_FIELD5    CHAR_FIELD16    LIST_FIELD19
   103    true    10000000003.0000000    some char text      [1]

Write ResultSet to Parquet File
===============================

In this example, we have the JDBC adapter result and trying to write them
into a parquet file.

.. testcode::

    import java.io.BufferedReader;
    import java.io.FileReader;
    import java.io.IOException;
    import java.nio.file.DirectoryStream;
    import java.nio.file.Files;
    import java.nio.file.Path;
    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Types;
    import java.util.HashMap;

    import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
    import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
    import org.apache.arrow.adapter.jdbc.JdbcToArrow;
    import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
    import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
    import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
    import org.apache.arrow.dataset.file.DatasetFileWriter;
    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VectorLoader;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.VectorUnloader;
    import org.apache.arrow.vector.ipc.ArrowReader;
    import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.ibatis.jdbc.ScriptRunner;

    class JDBCReader extends ArrowReader {
      private final ArrowVectorIterator iter;
      private final Schema schema;

      public JDBCReader(BufferAllocator allocator, ArrowVectorIterator iter, Schema schema) {
        super(allocator);
        this.iter = iter;
        this.schema = schema;
      }

      @Override
      public boolean loadNextBatch() throws IOException {
        while (iter.hasNext()) {
          try (VectorSchemaRoot rootTmp = iter.next()) {
            if (rootTmp.getRowCount() > 0) {
              VectorUnloader unloader = new VectorUnloader(rootTmp);
              VectorLoader loader = new VectorLoader(super.getVectorSchemaRoot());
              try (ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
                loader.load(recordBatch);
              }
              return true;
            }
            else {
              return false;
            }
          }
        }
        return false;
      }

      @Override
      public long bytesRead() {
        return 0;
      }

      @Override
      protected void closeReadSource() throws IOException {
      }

      @Override
      protected Schema readSchema() {
        return schema;
      }
    }
    final BufferAllocator allocator = new RootAllocator();
    try (
        final Connection connection = DriverManager.getConnection(
            "jdbc:h2:mem:h2-jdbc-adapter")
    ) {
      ScriptRunner runnerDDLDML = new ScriptRunner(connection);
      runnerDDLDML.setLogWriter(null);
      runnerDDLDML.runScript(new BufferedReader(
          new FileReader("./thirdpartydeps/jdbc/h2-ddl.sql")));
      runnerDDLDML.runScript(new BufferedReader(
          new FileReader("./thirdpartydeps/jdbc/h2-dml.sql")));
      JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(allocator,
          JdbcToArrowUtils.getUtcCalendar())
          .setTargetBatchSize(2)
          .setArraySubTypeByColumnNameMap(
              new HashMap() {{
                put("LIST_FIELD19",
                    new JdbcFieldInfo(Types.INTEGER));
              }}
          )
          .build();
      String query = "SELECT int_field1, bool_field2, bigint_field5, char_field16, list_field19 FROM TABLE1";
      try (
          final ResultSet resultSetConvertToParquet = connection.createStatement().executeQuery(query);
          final ResultSet resultSetForSchema = connection.createStatement().executeQuery(query);
          final ArrowVectorIterator arrowVectorIterator = JdbcToArrow.sqlToArrowVectorIterator(
              resultSetConvertToParquet, config)
      ) {
        Schema schema = JdbcToArrow.sqlToArrowVectorIterator(resultSetForSchema, config).next().getSchema();
        Path uri = Files.createTempDirectory("parquet_");
        try (
            // get jdbc row data as a arrow reader
            final JDBCReader arrowReader = new JDBCReader(allocator, arrowVectorIterator, schema)
        ) {
          // write arrow reader to parqueet file
          DatasetFileWriter.write(allocator, arrowReader, FileFormat.PARQUET, uri.toUri().toString());
        }
        // validate data of parquet file created
        ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
        try (
            DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator,
                NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri.toUri().toString());
            Dataset dataset = datasetFactory.finish();
            Scanner scanner = dataset.newScan(options);
            ArrowReader reader = scanner.scanBatches()
        ) {
          while (reader.loadNextBatch()) {
            System.out.print(reader.getVectorSchemaRoot().contentToTSVString());
          }
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        // delete temporary parquet file created
        try (DirectoryStream<Path> dir = Files.newDirectoryStream(uri)) {
          uri.toFile().deleteOnExit();
          for (Path path : dir) {
            path.toFile().deleteOnExit();
          }
        }
      }
      runnerDDLDML.closeConnection();
    } catch (SQLException | IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

.. testoutput::

   INT_FIELD1    BOOL_FIELD2    BIGINT_FIELD5    CHAR_FIELD16    LIST_FIELD19
   101    true    1000000000300    some char text      [1,2,3]
   102    true    100000000030    some char text      [1,2]
   INT_FIELD1    BOOL_FIELD2    BIGINT_FIELD5    CHAR_FIELD16    LIST_FIELD19
   103    true    10000000003    some char text      [1]
