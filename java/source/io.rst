.. _arrow-io:

========================
Reading and writing data
========================

Recipes related to reading and writing data from disk using Apache Arrow.

Arrow defines two types of binary formats for serializing record batches `IPC <https://arrow.apache.org/docs/java/ipc.html>`_: Streaming format / File or Random Access format

Arrow vectors can be serialized to disk as the Arrow IPC format. Such files can be directly memory-mapped when read.

.. contents::

Writing
=======

Both libraries writing random access file and streaming format offer the same API

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
    import java.io.FileNotFoundException;
    import java.io.FileOutputStream;
    import java.io.IOException;

    // Create and populate data:
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson = new Schema(asList(name, age));
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);
    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    nameVector.allocateNew(3);
    nameVector.set(0, "David".getBytes());
    nameVector.set(1, "Gladis".getBytes());
    nameVector.set(2, "Juan".getBytes());
    nameVector.setValueCount(3);
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    ageVector.setValueCount(3);
    vectorSchemaRoot.setRowCount(3);
    File file = new File("randon_access_to_file.arrow");
    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
         ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, /*WritableByteChannel out*/fileOutputStream.getChannel())
    ){
        writer.start();
        for (int i=0; i<10; i++){
            writer.writeBatch();
        }
        writer.end();
        System.out.println(writer.getRecordBlocks().size());
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

.. testoutput::

    10

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
    import java.io.FileNotFoundException;
    import java.io.IOException;
    import java.nio.channels.Channels;

    // Create and populate data:
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson = new Schema(asList(name, age));
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);
    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    nameVector.allocateNew(3);
    nameVector.set(0, "David".getBytes());
    nameVector.set(1, "Gladis".getBytes());
    nameVector.set(2, "Juan".getBytes());
    nameVector.setValueCount(3);
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    ageVector.setValueCount(3);
    vectorSchemaRoot.setRowCount(3);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, /*WritableByteChannel out*/Channels.newChannel(out)))
    {
        writer.start();
        for (int i=0; i<10; i++){
            // Generate data or modify the root or use a VectorLoader to get fresh data from somewhere else
            writer.writeBatch();
        }
        writer.end();
        System.out.println(writer.getRecordBlocks().size());
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

.. testoutput::

    10

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
    import java.io.FileNotFoundException;
    import java.io.FileOutputStream;
    import java.io.IOException;

    // Create and populate data:
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson = new Schema(asList(name, age));
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);
    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    nameVector.allocateNew(3);
    nameVector.set(0, "David".getBytes());
    nameVector.set(1, "Gladis".getBytes());
    nameVector.set(2, "Juan".getBytes());
    nameVector.setValueCount(3);
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    ageVector.setValueCount(3);
    vectorSchemaRoot.setRowCount(3);
    File file = new File("streaming_to_file.arrow");
    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
         ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, /*WritableByteChannel out*/fileOutputStream.getChannel())
    ){
        writer.start();
        for (int i=0; i<10; i++){
            // Generate data or modify the root or use a VectorLoader to get fresh data from somewhere else
            writer.writeBatch();
        }
        writer.end();
        System.out.println(writer.bytesWritten());
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

.. testoutput::

    2936

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
    import java.io.FileNotFoundException;
    import java.io.IOException;
    import java.nio.channels.Channels;

    // Create and populate data:
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson = new Schema(asList(name, age));
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);
    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    nameVector.allocateNew(3);
    nameVector.set(0, "David".getBytes());
    nameVector.set(1, "Gladis".getBytes());
    nameVector.set(2, "Juan".getBytes());
    nameVector.setValueCount(3);
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    ageVector.setValueCount(3);
    vectorSchemaRoot.setRowCount(3);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
         ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, /*WritableByteChannel out*/Channels.newChannel(out))
    ){
        writer.start();
        for (int i=0; i<10; i++){
            // Generate data or modify the root or use a VectorLoader to get fresh data from somewhere else
            writer.writeBatch();
        }
        writer.end();
        System.out.println(writer.bytesWritten());
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

.. testoutput::

    2936

Reading
=======

Reading the random access format and streaming format both offer the same API,
with the difference that random access files also offer access to any record batch by index.

Reading Random Access Files
***************************

Read - From File
----------------

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.ipc.message.ArrowBlock;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;
    import org.apache.arrow.vector.ipc.ArrowFileWriter;
    import java.io.File;
    import java.io.FileInputStream;
    import java.io.FileNotFoundException;
    import java.io.FileOutputStream;
    import java.io.IOException;
    import org.apache.arrow.vector.ipc.ArrowFileReader;

    // Read data
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson = new Schema(asList(name, age));
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);
    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    nameVector.allocateNew(3);
    nameVector.set(0, "David".getBytes());
    nameVector.set(1, "Gladis".getBytes());
    nameVector.set(2, "Juan".getBytes());
    nameVector.setValueCount(3);
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    ageVector.setValueCount(3);
    vectorSchemaRoot.setRowCount(3);
    File file = new File("randon_access_to_file.arrow");
    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
         ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, /*WritableByteChannel out*/fileOutputStream.getChannel())
    ){
        // write
        writer.start();
        for (int i=0; i<10; i++){
            // Generate data or modify the root or use a VectorLoader to get fresh data from somewhere else
            writer.writeBatch();
        }
        writer.end();

        // read
        try (FileInputStream fileInputStream = new FileInputStream(file);
             ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)
        ){
            // read the 2nd batch or the index that you need according to you write
            ArrowBlock block = reader.getRecordBlocks().get(1);
            reader.loadRecordBatch(block);
            VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
            System.out.print(vectorSchemaRootRecover.contentToTSVString());
        }

    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

.. testoutput::

    name    age
    David    10
    Gladis    20
    Juan    30

Read - From Buffer
------------------

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.ipc.ArrowFileReader;
    import org.apache.arrow.vector.ipc.SeekableReadChannel;
    import org.apache.arrow.vector.ipc.message.ArrowBlock;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;
    import org.apache.arrow.vector.ipc.ArrowFileWriter;
    import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

    import java.io.ByteArrayOutputStream;
    import java.io.FileNotFoundException;
    import java.io.IOException;
    import java.nio.channels.Channels;

    // Create and populate data:
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson = new Schema(asList(name, age));
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);
    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    nameVector.allocateNew(3);
    nameVector.set(0, "David".getBytes());
    nameVector.set(1, "Gladis".getBytes());
    nameVector.set(2, "Juan".getBytes());
    nameVector.setValueCount(3);
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    ageVector.setValueCount(3);
    vectorSchemaRoot.setRowCount(3);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
         ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, /*WritableByteChannel out*/Channels.newChannel(out)))
    {
        // write
        writer.start();
        for (int i=0; i<10; i++){
            // Generate data or modify the root or use a VectorLoader to get fresh data from somewhere else
            writer.writeBatch();
        }
        writer.end();

        // read
        try (ArrowFileReader reader = new ArrowFileReader(new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(out.toByteArray())), rootAllocator)
        ){
            // read the 2nd batch or the index that you need according to you write
            ArrowBlock block = reader.getRecordBlocks().get(1);
            reader.loadRecordBatch(block);
            VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
            System.out.print(vectorSchemaRootRecover.contentToTSVString());
        }
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

.. testoutput::

    name    age
    David    10
    Gladis    20
    Juan    30

Reading Streaming Format
************************

Read - From File
----------------

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.ipc.ArrowStreamReader;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;
    import org.apache.arrow.vector.ipc.ArrowStreamWriter;
    import java.io.File;
    import java.io.FileInputStream;
    import java.io.FileNotFoundException;
    import java.io.FileOutputStream;
    import java.io.IOException;

    // Create and populate data:
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson = new Schema(asList(name, age));
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);
    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    nameVector.allocateNew(3);
    nameVector.set(0, "David".getBytes());
    nameVector.set(1, "Gladis".getBytes());
    nameVector.set(2, "Juan".getBytes());
    nameVector.setValueCount(3);
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    ageVector.setValueCount(3);
    vectorSchemaRoot.setRowCount(3);
    File file = new File("streaming_to_file.arrow");
    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
         ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, /*WritableByteChannel out*/fileOutputStream.getChannel())
    ){
        // write
        writer.start();
        for (int i=0; i<10; i++){
            // Generate data or modify the root or use a VectorLoader to get fresh data from somewhere else
            writer.writeBatch();
        }
        writer.end();

        // read
        try (FileInputStream fileInputStreamForStream = new FileInputStream(file);
             ArrowStreamReader reader = new ArrowStreamReader(fileInputStreamForStream, rootAllocator)){
            // read the batch
            reader.loadNextBatch();
            VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
            System.out.print(vectorSchemaRootRecover.contentToTSVString());
        }
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

.. testoutput::

    name    age
    David    10
    Gladis    20
    Juan    30

Read - From Buffer
------------------

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.ipc.ArrowStreamReader;
    import org.apache.arrow.vector.ipc.ArrowStreamWriter;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;

    import java.io.ByteArrayInputStream;
    import java.io.ByteArrayOutputStream;
    import java.io.FileNotFoundException;
    import java.io.IOException;
    import java.nio.channels.Channels;

    // Create and populate data:
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson = new Schema(asList(name, age));
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);
    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    nameVector.allocateNew(3);
    nameVector.set(0, "David".getBytes());
    nameVector.set(1, "Gladis".getBytes());
    nameVector.set(2, "Juan".getBytes());
    nameVector.setValueCount(3);
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    ageVector.setValueCount(3);
    vectorSchemaRoot.setRowCount(3);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
         ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, /*WritableByteChannel out*/Channels.newChannel(out))
    ){
        // write
        writer.start();
        for (int i=0; i<10; i++){
            // Generate data or modify the root or use a VectorLoader to get fresh data from somewhere else
            writer.writeBatch();
        }
        writer.end();

        // read
        try (ArrowStreamReader readerBufferForStream = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), rootAllocator)
        ){
            readerBufferForStream.loadNextBatch();
            VectorSchemaRoot vectorSchemaRootRecover = readerBufferForStream.getVectorSchemaRoot();
            System.out.print(vectorSchemaRootRecover.contentToTSVString());
        }
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

.. testoutput::

    name    age
    David    10
    Gladis    20
    Juan    30