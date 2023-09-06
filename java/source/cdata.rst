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

================
C Data Interface
================

Recipes related to how to exchange data using C Data Interface specification.

.. contents::

Python (Consumer) - Java (Producer)
===================================

    For Python Consumer and Java Producer, please consider:

    - The Root Allocator should be shared for all memory allocations.

    - The Python application will sometimes shut down the Java JVM but Java JNI C Data will still work on releasing exported objects, which is why some guards have been implemented to protect against such scenarios. A warning message "WARNING: Failed to release Java C Data resource" indicates this scenario.

    - We do not know when `RootAllocator` will be closed. It is for this reason that the `RootAllocator` should survive so long as the export/import of used objects is released. Here is an example of this scenario:

        + Whenever Java code calls `allocator.close`, a memory leak will occur since many objects will have to be released on either Python or Java JNI sides.

        + To solve memory leak problems, you will call Java `allocator.close` when Python and Java JNI have released all their objects, which is impossible to accomplish.

    - In addition, Java applications should expose a method for closing all Java-created objects independently from Root Allocators.


Sharing ValueVector
*******************

Java Side:

.. testcode::

    import org.apache.arrow.c.ArrowArray;
    import org.apache.arrow.c.ArrowSchema;
    import org.apache.arrow.c.Data;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;
    import org.apache.arrow.vector.FieldVector;
    import org.apache.arrow.vector.IntVector;

    public class ShareValueVectorAPI {
      final static BufferAllocator allocator = new RootAllocator();
      final static IntVector intVector =
          new IntVector("to_be_consumed_by_python", allocator);

      public static BufferAllocator getAllocatorForJavaConsumers() {
        return allocator;
      }

      public static IntVector getIntVectorForJavaConsumers() {
        intVector.allocateNew(3);
        intVector.set(0, 1);
        intVector.set(1, 7);
        intVector.set(2, 93);
        intVector.setValueCount(3);
        return intVector;
      }

      public static void closeAllocatorForJavaConsumers() {
        try {
          AutoCloseables.close(AutoCloseables.iter(intVector));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      public static void simulateAsAJavaConsumers() {
        try (
            ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator)
        ) {
          Data.exportVector(allocator, getIntVectorForJavaConsumers(), null, arrowArray, arrowSchema);
          try (FieldVector valueVectors = Data.importVector(allocator, arrowArray, arrowSchema, null);) {
            System.out.print(valueVectors);
          }
        }
        closeAllocatorForJavaConsumers();
        allocator.close();
      }
    }

    ShareValueVectorAPI.simulateAsAJavaConsumers();

.. testoutput::

   [1, 7, 93]

Python Side:

.. code-block:: python

    import jpype
    import pyarrow as pa
    from pyarrow.cffi import ffi

    jvmargs=["-Darrow.memory.debug.allocator=true"]
    jpype.startJVM(*jvmargs, jvmpath=jpype.getDefaultJVMPath(), classpath=[
        "./target/java-python-by-cdata-1.0-SNAPSHOT-jar-with-dependencies.jar"])
    java_value_vector_api = jpype.JClass('ShareValueVectorAPI')
    java_c_package = jpype.JPackage("org").apache.arrow.c
    py_c_schema = ffi.new("struct ArrowSchema*")
    py_ptr_schema = int(ffi.cast("uintptr_t", py_c_schema))
    py_c_array = ffi.new("struct ArrowArray*")
    py_ptr_array = int(ffi.cast("uintptr_t", py_c_array))
    java_wrapped_schema = java_c_package.ArrowSchema.wrap(py_ptr_schema)
    java_wrapped_array = java_c_package.ArrowArray.wrap(py_ptr_array)
    java_c_package.Data.exportVector(
        java_value_vector_api.getAllocatorForJavaConsumers(),
        java_value_vector_api.getIntVectorForJavaConsumers(),
        None,
        java_wrapped_array,
        java_wrapped_schema
    )
    py_array = pa.Array._import_from_c(py_ptr_array, py_ptr_schema)
    print(type(py_array))
    print(py_array)

.. code-block:: shell

    <class 'pyarrow.lib.Int32Array'>
    [
      1,
      7,
      93
    ]

Sharing VectorSchemaRoot
************************

Java Side:

.. testcode::

    import org.apache.arrow.c.ArrowArray;
    import org.apache.arrow.c.ArrowSchema;
    import org.apache.arrow.c.Data;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.Schema;

    import static java.util.Arrays.asList;

    public class ShareVectorSchemaRootAPI {
      final static BufferAllocator allocator = new RootAllocator();
      final static Field column_one = new Field("column-one", FieldType.nullable(new ArrowType.Int(32, true)), null);
      final static Schema schema = new Schema(asList(column_one));
      final static VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

      public static BufferAllocator getAllocatorForJavaConsumers() {
        return allocator;
      }

      public static VectorSchemaRoot getVectorSchemaRootForJavaConsumers() {
        IntVector intVector = (IntVector) root.getVector(0);
        root.allocateNew();
        intVector.set(0, 100);
        intVector.set(1, 20);
        root.setRowCount(2);
        return root;
      }

      public static void closeAllocatorForJavaConsumers() {
        try {
          AutoCloseables.close(AutoCloseables.iter(root));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      public static void simulateAsAJavaConsumers() {
        try (ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
             ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator)
        ) {
          Data.exportVectorSchemaRoot(allocator, getVectorSchemaRootForJavaConsumers(), null, arrowArray, arrowSchema);
          try (VectorSchemaRoot root = Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, null);) {
            System.out.print(root.contentToTSVString());
          }
        }
        closeAllocatorForJavaConsumers();
        allocator.close();
      }
    }

    ShareVectorSchemaRootAPI.simulateAsAJavaConsumers();

.. testoutput::

    column-one
    100
    20

Python Side:

.. code-block:: python

    import jpype
    import pyarrow as pa
    from pyarrow.cffi import ffi

    jvmargs=["-Darrow.memory.debug.allocator=true"]
    jpype.startJVM(*jvmargs, jvmpath=jpype.getDefaultJVMPath(), classpath=[
        "./target/java-python-by-cdata-1.0-SNAPSHOT-jar-with-dependencies.jar"])
    java_value_vector_api = jpype.JClass('ShareVectorSchemaRootAPI')
    java_c_package = jpype.JPackage("org").apache.arrow.c
    py_c_schema = ffi.new("struct ArrowSchema*")
    py_ptr_schema = int(ffi.cast("uintptr_t", py_c_schema))
    py_c_array = ffi.new("struct ArrowArray*")
    py_ptr_array = int(ffi.cast("uintptr_t", py_c_array))
    java_wrapped_schema = java_c_package.ArrowSchema.wrap(py_ptr_schema)
    java_wrapped_array = java_c_package.ArrowArray.wrap(py_ptr_array)
    java_c_package.Data.exportVectorSchemaRoot(
        java_value_vector_api.getAllocatorForJavaConsumers(),
        java_value_vector_api.getVectorSchemaRootForJavaConsumers(),
        None,
        java_wrapped_array,
        java_wrapped_schema
    )
    py_record_batch = pa.Array._import_from_c(py_ptr_array, py_ptr_schema)
    print(type(py_record_batch))
    print(py_record_batch)

.. code-block:: shell

    <class 'pyarrow.lib.StructArray'>
    -- is_valid: all not null
    -- child 0 type: int32
      [
        100,
        20
      ]

Sharing ArrowReader
*******************

Java Side:

.. testcode::

    import java.io.BufferedReader;
    import java.io.FileNotFoundException;
    import java.io.FileReader;
    import java.io.IOException;
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
    import org.apache.arrow.c.ArrowArrayStream;
    import org.apache.arrow.c.Data;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.ipc.ArrowReader;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.ibatis.jdbc.ScriptRunner;

    public class ShareArrowReaderAPI {
      final static BufferAllocator allocator = new RootAllocator();
      static Connection connection;
      static ScriptRunner runnerDDLDML;
      static ArrowVectorIterator arrowVectorIterator;
      static ArrowReader arrowReader;

      public static ArrowReader getArrowReaderForJavaConsumers(int batchSize, boolean reuseVSR) {
        try {
          connection = DriverManager.getConnection("jdbc:h2:mem:h2-jdbc-adapter");
          runnerDDLDML = new ScriptRunner(connection);
          runnerDDLDML.setLogWriter(null);
          runnerDDLDML.runScript(new BufferedReader(
              new FileReader("./thirdpartydeps/jdbc/h2-ddl.sql")));
          runnerDDLDML.runScript(new BufferedReader(
              new FileReader("./thirdpartydeps/jdbc/h2-dml.sql")));
          final JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(allocator,
              JdbcToArrowUtils.getUtcCalendar())
              .setTargetBatchSize(batchSize)
              .setReuseVectorSchemaRoot(reuseVSR)
              .setArraySubTypeByColumnNameMap(
                  new HashMap<>() {{
                    put("LIST_FIELD19",
                        new JdbcFieldInfo(Types.INTEGER));
                  }}
              )
              .build();
          final ResultSet resultSetConvertToParquet;
          String query = "SELECT int_field1, bool_field2, bigint_field5, char_field16, list_field19 FROM TABLE1";
          resultSetConvertToParquet = connection.createStatement().executeQuery(query);
          arrowVectorIterator = JdbcToArrow.sqlToArrowVectorIterator(
              resultSetConvertToParquet, config);
          arrowReader = new JDBCReader(allocator, arrowVectorIterator, config);
          return arrowReader;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
          throw new RuntimeException(e);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      public static void closeAllocatorForJavaConsumers() throws SQLException, IOException {
        runnerDDLDML.closeConnection();
        connection.close();
        arrowVectorIterator.close();
        arrowReader.close();
      }

      public static void simulateAsAJavaConsumers() throws IOException, SQLException {
        try (ArrowArrayStream arrowArrayStream = ArrowArrayStream.allocateNew(allocator)) {
          Data.exportArrayStream(allocator, getArrowReaderForJavaConsumers(/*batchSize*/ 2, /*reuseVSR*/ true), arrowArrayStream);
          try (ArrowReader arrowReader = Data.importArrayStream(allocator, arrowArrayStream)) {
            while (arrowReader.loadNextBatch()) {
              System.out.print(arrowReader.getVectorSchemaRoot().contentToTSVString());
            }
          }
        }
        closeAllocatorForJavaConsumers();
        allocator.close();
      }
    }

    class JDBCReader extends ArrowReader {
      private final ArrowVectorIterator iter;
      private final JdbcToArrowConfig config;
      private VectorSchemaRoot root;
      private boolean firstRoot = true;

      public JDBCReader(BufferAllocator allocator, ArrowVectorIterator iter, JdbcToArrowConfig config) {
        super(allocator);
        this.iter = iter;
        this.config = config;
      }

      @Override
      public boolean loadNextBatch() throws IOException {
        if (firstRoot) {
          firstRoot = false;
          return true;
        }
        else {
          if (iter.hasNext()) {
            if (root != null && !config.isReuseVectorSchemaRoot()) {
              root.close();
            }
            else {
              root.allocateNew();
            }
            root = iter.next();
            return root.getRowCount() != 0;
          }
          else {
            return false;
          }
        }
      }

      @Override
      public long bytesRead() {
        return -666;
      }

      @Override
      protected void closeReadSource() throws IOException {
        if (root != null && !config.isReuseVectorSchemaRoot()) {
          root.close();
        }
      }

      @Override
      protected Schema readSchema() throws IOException {
        return null;
      }

      @Override
      public VectorSchemaRoot getVectorSchemaRoot() throws IOException {
        if (root == null) {
          root = iter.next();
        }
        return root;
      }

      @Override
      public void close() throws IOException {
        super.close();
      }
    }

    ShareArrowReaderAPI.simulateAsAJavaConsumers();

.. testoutput::

    INT_FIELD1    BOOL_FIELD2    BIGINT_FIELD5    CHAR_FIELD16    LIST_FIELD19
    101    true    1000000000300    some char text      [1,2,3]
    102    true    100000000030    some char text      [1,2]
    INT_FIELD1    BOOL_FIELD2    BIGINT_FIELD5    CHAR_FIELD16    LIST_FIELD19
    103    true    10000000003    some char text      [1]

Python Side:

.. code-block:: python

    import jpype
    import pyarrow as pa
    import pyarrow.dataset as ds
    import sys
    from pyarrow.cffi import ffi

    def getRecordBatchReader(py_stream_ptr):
        generator = getIterableRecordBatchReader(py_stream_ptr)
        schema = next(generator)
        return pa.RecordBatchReader.from_batches(schema, generator)

    def getIterableRecordBatchReader(py_stream_ptr):
        with pa.RecordBatchReader._import_from_c(py_stream_ptr) as reader: #Import Schema from a C ArrowSchema struct, given its pointer.
            yield reader.schema
            yield from reader

    jvmargs=["-Darrow.memory.debug.allocator=true"]
    jpype.startJVM(*jvmargs, jvmpath=jpype.getDefaultJVMPath(), classpath=[
        "./target/java-python-by-cdata-1.0-SNAPSHOT-jar-with-dependencies.jar"])
    java_reader_api = jpype.JClass('ShareArrowReaderAPI')
    java_c_package = jpype.JPackage("org").apache.arrow.c
    py_stream = ffi.new("struct ArrowArrayStream*")
    py_stream_ptr = int(ffi.cast("uintptr_t", py_stream))
    java_wrapped_stream = java_c_package.ArrowArrayStream.wrap(py_stream_ptr)
    java_c_package.Data.exportArrayStream(
        java_reader_api.getAllocatorForJavaConsumers(),
        java_reader_api.getArrowReaderForJavaConsumers(int(sys.argv[1]), # batchSize = int(sys.argv[1])
                                                       eval(sys.argv[2])), # reuseVSR = eval(sys.argv[2]
        java_wrapped_stream)

    with getRecordBatchReader(py_stream_ptr) as streamsReaderForJava:
        ds.write_dataset(streamsReaderForJava,
                         './jdbc/parquet',
                         format="parquet")

    java_reader_api.closeAllocatorForJavaConsumers();

.. code-block:: shell

    parquet-tools cat ./jdbc/parquet/part-0.parquet

    INT_FIELD1 = 101
    BOOL_FIELD2 = true
    BIGINT_FIELD5 = 1000000000300
    CHAR_FIELD16 = some char text
    LIST_FIELD19:
    .list:
    ..child = 1
    .list:
    ..child = 2
    .list:
    ..child = 3

    INT_FIELD1 = 102
    BOOL_FIELD2 = true
    BIGINT_FIELD5 = 100000000030
    CHAR_FIELD16 = some char text
    LIST_FIELD19:
    .list:
    ..child = 1
    .list:
    ..child = 2

    INT_FIELD1 = 103
    BOOL_FIELD2 = true
    BIGINT_FIELD5 = 10000000003
    CHAR_FIELD16 = some char text
    LIST_FIELD19:
    .list:
    ..child = 1




