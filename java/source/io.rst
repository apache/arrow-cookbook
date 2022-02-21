.. _arrow-io:

========================
Reading and writing data
========================

The `Arrow IPC format <https://arrow.apache.org/docs/java/ipc.html>`_ defines two types of binary formats
for serializing Arrow data: the streaming format and the file format (or random access format). Such files can
be directly memory-mapped when read.

.. contents::

Writing
=======

Both writing file and streaming formats use the same API.

Writing Random Access Files
***************************

Write - Out to File
-------------------

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;
    import org.apache.arrow.vector.ipc.ArrowFileWriter;

    import java.io.File;
    import java.io.FileOutputStream;
    import java.io.IOException;

    try (RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)) {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schemaPerson = new Schema(asList(name, age));
        try(VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator)){
            VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
            nameVector.allocateNew(3);
            nameVector.set(0, "David".getBytes());
            nameVector.set(1, "Gladis".getBytes());
            nameVector.set(2, "Juan".getBytes());
            IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
            ageVector.allocateNew(3);
            ageVector.set(0, 10);
            ageVector.set(1, 20);
            ageVector.set(2, 30);
            vectorSchemaRoot.setRowCount(3);
            File file = new File("randon_access_to_file.arrow");
            try (FileOutputStream fileOutputStream = new FileOutputStream(file);
                 ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel())
            ) {
                writer.start();
                writer.writeBatch();
                writer.end();
                System.out.println("Record batches written: " + writer.getRecordBlocks().size() + ". Number of rows written: " + vectorSchemaRoot.getRowCount());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

.. testoutput::

    Record batches written: 1. Number of rows written: 3

Write - Out to Buffer
---------------------

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;
    import org.apache.arrow.vector.ipc.ArrowFileWriter;

    import java.io.ByteArrayOutputStream;
    import java.io.IOException;
    import java.nio.channels.Channels;

    try (RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)) {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schemaPerson = new Schema(asList(name, age));
        try(VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator)){
            VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
            nameVector.allocateNew(3);
            nameVector.set(0, "David".getBytes());
            nameVector.set(1, "Gladis".getBytes());
            nameVector.set(2, "Juan".getBytes());
            IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
            ageVector.allocateNew(3);
            ageVector.set(0, 10);
            ageVector.set(1, 20);
            ageVector.set(2, 30);
            vectorSchemaRoot.setRowCount(3);
            try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                 ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, Channels.newChannel(out)))
            {
                writer.start();
                writer.writeBatch();
                System.out.println("Record batches written: " + writer.getRecordBlocks().size() + ". Number of rows written: " + vectorSchemaRoot.getRowCount());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

.. testoutput::

    Record batches written: 1. Number of rows written: 3

Writing Streaming Format
************************

Write - Out to File
-------------------

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;
    import org.apache.arrow.vector.ipc.ArrowStreamWriter;
    import java.io.File;
    import java.io.FileOutputStream;
    import java.io.IOException;

    try (RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)) {
        // Create and populate data:
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schemaPerson = new Schema(asList(name, age));
        try(VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator)){
            VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
            nameVector.allocateNew(3);
            nameVector.set(0, "David".getBytes());
            nameVector.set(1, "Gladis".getBytes());
            nameVector.set(2, "Juan".getBytes());
            IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
            ageVector.allocateNew(3);
            ageVector.set(0, 10);
            ageVector.set(1, 20);
            ageVector.set(2, 30);
            vectorSchemaRoot.setRowCount(3);
            File file = new File("streaming_to_file.arrow");
            try (FileOutputStream fileOutputStream = new FileOutputStream(file);
                 ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, fileOutputStream.getChannel())
            ){
                writer.start();
                writer.writeBatch();
                System.out.println("Number of rows written: " + vectorSchemaRoot.getRowCount());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

.. testoutput::

    Number of rows written: 3

Write - Out to Buffer
---------------------

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.ipc.ArrowStreamWriter;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;

    import java.io.ByteArrayOutputStream;
    import java.io.IOException;
    import java.nio.channels.Channels;

    try (RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)) {
        // Create and populate data:
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schemaPerson = new Schema(asList(name, age));
        try(VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator)){
            VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
            nameVector.allocateNew(3);
            nameVector.set(0, "David".getBytes());
            nameVector.set(1, "Gladis".getBytes());
            nameVector.set(2, "Juan".getBytes());
            IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
            ageVector.allocateNew(3);
            ageVector.set(0, 10);
            ageVector.set(1, 20);
            ageVector.set(2, 30);
            vectorSchemaRoot.setRowCount(3);
            try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                 ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, Channels.newChannel(out))
            ){
                writer.start();
                writer.writeBatch();
                System.out.println("Number of rows written: " + vectorSchemaRoot.getRowCount());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

.. testoutput::

    Number of rows written: 3

Reading
=======

Reading the random access format and streaming format both offer the same API,
with the difference that random access files also offer access to any record batch by index.

Reading Random Access Files
***************************

Read - From File
----------------

We are providing a path with auto generated arrow files for testing purposes, change that at your convenience.

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowFileReader;
    import org.apache.arrow.vector.ipc.message.ArrowBlock;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import java.io.File;
    import java.io.FileInputStream;
    import java.io.FileOutputStream;
    import java.io.IOException;

    try(RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)){
        File file = new File("./thirdpartydeps/arrowfiles/random_access.arrow");
        try (FileInputStream fileInputStream = new FileInputStream(file);
             ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)
        ){
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                System.out.print(vectorSchemaRootRecover.contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

.. testoutput::

    Record batches in file: 3
    name    age
    David    10
    Gladis    20
    Juan    30
    name    age
    Nidia    15
    Alexa    20
    Mara    15
    name    age
    Raul    34
    Jhon    29
    Thomy    33

Read - From Buffer
------------------

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowFileReader;
    import org.apache.arrow.vector.ipc.SeekableReadChannel;
    import org.apache.arrow.vector.ipc.message.ArrowBlock;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

    import java.io.IOException;
    import java.nio.file.Files;
    import java.nio.file.Path;
    import java.nio.file.Paths;

    try(RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)) {
        Path path = Paths.get("./thirdpartydeps/arrowfiles/random_access.arrow");
        try (ArrowFileReader reader = new ArrowFileReader(new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(Files.readAllBytes(path))), rootAllocator)){
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                System.out.print(vectorSchemaRootRecover.contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

.. testoutput::

    Record batches in file: 3
    name    age
    David    10
    Gladis    20
    Juan    30
    name    age
    Nidia    15
    Alexa    20
    Mara    15
    name    age
    Raul    34
    Jhon    29
    Thomy    33

Reading Streaming Format
************************

Read - From File
----------------

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowStreamReader;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import java.io.File;
    import java.io.FileInputStream;
    import java.io.IOException;

    try(RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)) {
        File file = new File("./thirdpartydeps/arrowfiles/streaming.arrow");
        try (FileInputStream fileInputStreamForStream = new FileInputStream(file);
             ArrowStreamReader reader = new ArrowStreamReader(fileInputStreamForStream, rootAllocator)) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                System.out.print(vectorSchemaRootRecover.contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

.. testoutput::

    name    age
    David    10
    Gladis    20
    Juan    30
    name    age
    Nidia    15
    Alexa    20
    Mara    15
    name    age
    Raul    34
    Jhon    29
    Thomy    33

Read - From Buffer
------------------

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowStreamReader;

    import java.io.ByteArrayInputStream;
    import java.io.IOException;
    import java.nio.file.Files;
    import java.nio.file.Path;
    import java.nio.file.Paths;

    try(RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)) {
        Path path = Paths.get("./thirdpartydeps/arrowfiles/streaming.arrow");
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(Files.readAllBytes(path)), rootAllocator)){
            while(reader.loadNextBatch()){
                System.out.print(reader.getVectorSchemaRoot().contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

.. testoutput::

    name    age
    David    10
    Gladis    20
    Juan    30
    name    age
    Nidia    15
    Alexa    20
    Mara    15
    name    age
    Raul    34
    Jhon    29
    Thomy    33

Reading Parquet File
********************

Please check :doc:`Dataset <./dataset>`
