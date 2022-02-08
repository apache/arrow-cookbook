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
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    vectorSchemaRoot.setRowCount(3);
    File file = new File("randon_access_to_file.arrow");
    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
         ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel())
    ){
        writer.start();
        for (int i=0; i<10; i++){
            // Generate data or modify the root or use a VectorLoader to get fresh data from somewhere else
            writer.writeBatch();
        }
        System.out.println("Record batches written: " + writer.getRecordBlocks().size());
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

.. testoutput::

    Record batches written: 10

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
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    vectorSchemaRoot.setRowCount(3);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, /*WritableByteChannel out*/Channels.newChannel(out)))
    {
        writer.start();
        for (int i=0; i<10; i++){
            // Generate data or modify the root or use a VectorLoader to get fresh data from somewhere else
            writer.writeBatch();
        }
        System.out.println("Record batches written: " + writer.getRecordBlocks().size());
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

.. testoutput::

    Record batches written: 10

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
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
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
        System.out.println(writer.bytesWritten());
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

.. testoutput::

    2928

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
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    vectorSchemaRoot.setRowCount(3);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
         ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, /*WritableByteChannel out*/Channels.newChannel(out))
    ){
        writer.start();
        for (int i=0; i<10; i++){
            // Generate data or modify the root or use a VectorLoader to get fresh data from somewhere else
            writer.writeBatch();
        }
        System.out.println(writer.bytesWritten());
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

.. testoutput::

    2928

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
    import org.apache.arrow.vector.VectorLoader;
    import org.apache.arrow.vector.VectorUnloader;
    import org.apache.arrow.vector.ipc.ArrowFileReader;
    import org.apache.arrow.vector.ipc.ArrowFileWriter;
    import org.apache.arrow.vector.ipc.ArrowStreamReader;
    import org.apache.arrow.vector.ipc.ArrowStreamWriter;
    import org.apache.arrow.vector.ipc.message.ArrowBlock;
    import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;
    import java.io.File;
    import java.io.FileInputStream;
    import java.io.FileNotFoundException;
    import java.io.FileOutputStream;
    import java.io.IOException;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    // Create and populate data:
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson = new Schema(asList(name, age));
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);
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
    Field name2 = new Field("name2", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age2 = new Field("age2", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson2 = new Schema(asList(name2, age2));
    VectorSchemaRoot vectorSchemaRoot2 = VectorSchemaRoot.create(schemaPerson2, rootAllocator);
    VarCharVector nameVector2 = (VarCharVector) vectorSchemaRoot2.getVector("name2");
    nameVector2.allocateNew(3);
    nameVector2.set(0, "Nidia".getBytes());
    nameVector2.set(1, "Alexa".getBytes());
    nameVector2.set(2, "Mara".getBytes());
    IntVector ageVector2 = (IntVector) vectorSchemaRoot2.getVector("age2");
    ageVector2.allocateNew(3);
    ageVector2.set(0, 15);
    ageVector2.set(1, 20);
    ageVector2.set(2, 15);
    vectorSchemaRoot2.setRowCount(3);
    Field name3 = new Field("name3", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age3 = new Field("age3", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson3 = new Schema(asList(name3, age3));
    VectorSchemaRoot vectorSchemaRoot3 = VectorSchemaRoot.create(schemaPerson3, rootAllocator);
    VarCharVector nameVector3 = (VarCharVector) vectorSchemaRoot3.getVector("name3");
    nameVector3.allocateNew(3);
    nameVector3.set(0, "Raul".getBytes());
    nameVector3.set(1, "Jhon".getBytes());
    nameVector3.set(2, "Thomy".getBytes());
    IntVector ageVector3 = (IntVector) vectorSchemaRoot3.getVector("age3");
    ageVector3.allocateNew(3);
    ageVector3.set(0, 34);
    ageVector3.set(1, 29);
    ageVector3.set(2, 33);
    vectorSchemaRoot3.setRowCount(3);
    File file = new File("randon_access_to_file.arrow");
    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
         ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, /*WritableByteChannel out*/fileOutputStream.getChannel())
    ){
        // write
        writer.start();
        for (int i=0; i<3; i++){
            // Generate data or modify the root or use a VectorLoader to get fresh data from somewhere else
            if (i==1){
                VectorUnloader vectorUnloader2 = new VectorUnloader(vectorSchemaRoot2);
                ArrowRecordBatch arrowRecordBatch2 = vectorUnloader2.getRecordBatch();
                VectorLoader vectorLoader2 = new VectorLoader(vectorSchemaRoot);
                vectorLoader2.load(arrowRecordBatch2);
            }
            if (i==2){
                VectorUnloader vectorUnloader3 = new VectorUnloader(vectorSchemaRoot3);
                ArrowRecordBatch arrowRecordBatch3 = vectorUnloader3.getRecordBatch();
                VectorLoader vectorLoader3 = new VectorLoader(vectorSchemaRoot);
                vectorLoader3.load(arrowRecordBatch3);
            }
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
    Nidia    15
    Alexa    20
    Mara    15

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

    // Create and populate data
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
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
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
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    vectorSchemaRoot.setRowCount(3);
    File file = new File("streaming_to_file.arrow");
    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
         ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, /*WritableByteChannel out*/fileOutputStream.getChannel())
    ){
        // write
        writer.start();
        for (int i=0; i<2; i++){
            // Generate data or modify the root or use a VectorLoader to get fresh data from somewhere else
            writer.writeBatch();
        }

        // read
        try (FileInputStream fileInputStreamForStream = new FileInputStream(file);
             ArrowStreamReader reader = new ArrowStreamReader(fileInputStreamForStream, rootAllocator)){
            while(reader.loadNextBatch()){
                // read the batch (on the next example you could see how to use a VectorLoader to get fresh data)
                VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                System.out.print(vectorSchemaRootRecover.contentToTSVString());
            }
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
    import org.apache.arrow.vector.VectorLoader;
    import org.apache.arrow.vector.VectorUnloader;
    import org.apache.arrow.vector.ipc.ArrowStreamReader;
    import org.apache.arrow.vector.ipc.ArrowStreamWriter;
    import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;

    import static java.util.Arrays.asList;
    import java.io.File;
    import java.io.FileInputStream;
    import java.io.FileNotFoundException;
    import java.io.FileOutputStream;
    import java.io.IOException;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    // Create and populate data:
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson = new Schema(asList(name, age));
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);
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
    Field name2 = new Field("name2", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age2 = new Field("age2", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson2 = new Schema(asList(name2, age2));
    VectorSchemaRoot vectorSchemaRoot2 = VectorSchemaRoot.create(schemaPerson2, rootAllocator);
    VarCharVector nameVector2 = (VarCharVector) vectorSchemaRoot2.getVector("name2");
    nameVector2.allocateNew(3);
    nameVector2.set(0, "Nidia".getBytes());
    nameVector2.set(1, "Alexa".getBytes());
    nameVector2.set(2, "Mara".getBytes());
    IntVector ageVector2 = (IntVector) vectorSchemaRoot2.getVector("age2");
    ageVector2.allocateNew(3);
    ageVector2.set(0, 15);
    ageVector2.set(1, 20);
    ageVector2.set(2, 15);
    vectorSchemaRoot2.setRowCount(3);
    Field name3 = new Field("name3", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age3 = new Field("age3", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson3 = new Schema(asList(name3, age3));
    VectorSchemaRoot vectorSchemaRoot3 = VectorSchemaRoot.create(schemaPerson3, rootAllocator);
    VarCharVector nameVector3 = (VarCharVector) vectorSchemaRoot3.getVector("name3");
    nameVector3.allocateNew(3);
    nameVector3.set(0, "Raul".getBytes());
    nameVector3.set(1, "Jhon".getBytes());
    nameVector3.set(2, "Thomy".getBytes());
    IntVector ageVector3 = (IntVector) vectorSchemaRoot3.getVector("age3");
    ageVector3.allocateNew(3);
    ageVector3.set(0, 34);
    ageVector3.set(1, 29);
    ageVector3.set(2, 33);
    vectorSchemaRoot3.setRowCount(3);
    File file = new File("streaming_to_file.arrow");
    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
         ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, /*WritableByteChannel out*/fileOutputStream.getChannel())
    ){
        // write
        writer.start();
        for (int i=0; i<3; i++){
            // Generate data or modify the root or use a VectorLoader to get fresh data from somewhere else
            if (i==1){
                VectorUnloader vectorUnloader2 = new VectorUnloader(vectorSchemaRoot2);
                ArrowRecordBatch arrowRecordBatch2 = vectorUnloader2.getRecordBatch();
                VectorLoader vectorLoader2 = new VectorLoader(vectorSchemaRoot);
                vectorLoader2.load(arrowRecordBatch2);
            }
            if (i==2){
                VectorUnloader vectorUnloader3 = new VectorUnloader(vectorSchemaRoot3);
                ArrowRecordBatch arrowRecordBatch3 = vectorUnloader3.getRecordBatch();
                VectorLoader vectorLoader3 = new VectorLoader(vectorSchemaRoot);
                vectorLoader3.load(arrowRecordBatch3);
            }
            writer.writeBatch();
        }

        // read
        try (FileInputStream fileInputStreamForStream = new FileInputStream(file);
             ArrowStreamReader reader = new ArrowStreamReader(fileInputStreamForStream, rootAllocator)){
            while(reader.loadNextBatch()){
                // read the batch
                System.out.print(reader.getVectorSchemaRoot().contentToTSVString());
            }
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
    name    age
    Nidia    15
    Alexa    20
    Mara    15
    name    age
    Raul    34
    Jhon    29
    Thomy    33
