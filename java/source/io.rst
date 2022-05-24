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

    import org.apache.arrow.memory.BufferAllocator;
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

    try (BufferAllocator allocator = new RootAllocator()) {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schemaPerson = new Schema(asList(name, age));
        try(
            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, allocator)
        ){
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
            try (
                FileOutputStream fileOutputStream = new FileOutputStream(file);
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

    import org.apache.arrow.memory.BufferAllocator;
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

    try (BufferAllocator allocator = new RootAllocator()) {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schemaPerson = new Schema(asList(name, age));
        try(
            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, allocator)
        ){
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
            try (
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                 ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, Channels.newChannel(out))
            ) {
                writer.start();
                writer.writeBatch();

                System.out.println("Record batches written: " + writer.getRecordBlocks().size() +
                        ". Number of rows written: " + vectorSchemaRoot.getRowCount());
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

    import org.apache.arrow.memory.BufferAllocator;
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
    import java.io.File;
    import java.io.FileOutputStream;
    import java.io.IOException;

    try (BufferAllocator rootAllocator = new RootAllocator()) {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schemaPerson = new Schema(asList(name, age));
        try(
            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator)
        ){
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
            try (
                FileOutputStream fileOutputStream = new FileOutputStream(file);
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

    import org.apache.arrow.memory.BufferAllocator;
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

    try (BufferAllocator rootAllocator = new RootAllocator()) {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schemaPerson = new Schema(asList(name, age));
        try(
            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator)
        ){
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
            try (
                ByteArrayOutputStream out = new ByteArrayOutputStream();
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

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowFileReader;
    import org.apache.arrow.vector.ipc.message.ArrowBlock;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import java.io.File;
    import java.io.FileInputStream;
    import java.io.IOException;

    File file = new File("./thirdpartydeps/arrowfiles/random_access.arrow");
    try(
        BufferAllocator rootAllocator = new RootAllocator();
        FileInputStream fileInputStream = new FileInputStream(file);
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

    import org.apache.arrow.memory.BufferAllocator;
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

    Path path = Paths.get("./thirdpartydeps/arrowfiles/random_access.arrow");
    try(
        BufferAllocator rootAllocator = new RootAllocator();
        ArrowFileReader reader = new ArrowFileReader(new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(
                                            Files.readAllBytes(path))), rootAllocator)
    ) {
        System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
        for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
            reader.loadRecordBatch(arrowBlock);
            VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
            System.out.print(vectorSchemaRootRecover.contentToTSVString());
        }
    } catch (IOException e) {
        e.printStackTrace();
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

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowStreamReader;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import java.io.File;
    import java.io.FileInputStream;
    import java.io.IOException;

    File file = new File("./thirdpartydeps/arrowfiles/streaming.arrow");
    try(
        BufferAllocator rootAllocator = new RootAllocator();
        FileInputStream fileInputStreamForStream = new FileInputStream(file);
        ArrowStreamReader reader = new ArrowStreamReader(fileInputStreamForStream, rootAllocator)
    ) {
        while (reader.loadNextBatch()) {
            VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
            System.out.print(vectorSchemaRootRecover.contentToTSVString());
        }
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

Read - From Buffer
------------------

.. testcode::

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowStreamReader;
    import java.io.ByteArrayInputStream;
    import java.io.IOException;
    import java.nio.file.Files;
    import java.nio.file.Path;
    import java.nio.file.Paths;

    Path path = Paths.get("./thirdpartydeps/arrowfiles/streaming.arrow");
    try(
        BufferAllocator rootAllocator = new RootAllocator();
        ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(
                                        Files.readAllBytes(path)), rootAllocator)
    ) {
        while(reader.loadNextBatch()){
            System.out.print(reader.getVectorSchemaRoot().contentToTSVString());
        }
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

Reading Parquet File
********************

Please check :doc:`Dataset <./dataset>`

Handling Data with Dictionaries
*******************************

Reading and writing dictionary-encoded data requires separately tracking the dictionaries.

.. testcode::

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.FieldVector;
    import org.apache.arrow.vector.ValueVector;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.dictionary.Dictionary;
    import org.apache.arrow.vector.dictionary.DictionaryEncoder;
    import org.apache.arrow.vector.dictionary.DictionaryProvider;
    import org.apache.arrow.vector.ipc.ArrowFileReader;
    import org.apache.arrow.vector.ipc.ArrowFileWriter;
    import org.apache.arrow.vector.ipc.message.ArrowBlock;
    import org.apache.arrow.vector.types.pojo.DictionaryEncoding;

    import java.io.File;
    import java.io.FileInputStream;
    import java.io.FileNotFoundException;
    import java.io.FileOutputStream;
    import java.io.IOException;
    import java.nio.charset.StandardCharsets;

    try (BufferAllocator root = new RootAllocator();
         VarCharVector countries = new VarCharVector("country-dict", root);
         VarCharVector myAppUseCountryDictionary = new VarCharVector("app-use-country-dict", root)
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

        Dictionary myCountryDictionary = new Dictionary(countries,
                new DictionaryEncoding(/*id=*/123L, /*ordered=*/false, /*indexType=*/null));
        System.out.println("Dictionary used: " + myCountryDictionary);

        myAppUseCountryDictionary.allocateNew(5);
        myAppUseCountryDictionary.set(0, "Andorra".getBytes(StandardCharsets.UTF_8));
        myAppUseCountryDictionary.set(1, "Guinea".getBytes(StandardCharsets.UTF_8));
        myAppUseCountryDictionary.set(2, "Islandia".getBytes(StandardCharsets.UTF_8));
        myAppUseCountryDictionary.set(3, "Malta".getBytes(StandardCharsets.UTF_8));
        myAppUseCountryDictionary.set(4, "Uganda".getBytes(StandardCharsets.UTF_8));
        myAppUseCountryDictionary.setValueCount(5);
        System.out.println("Data to retain: " + myAppUseCountryDictionary);

        File file = new File("random_access_file_with_dictionary.arrow");
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        provider.put(myCountryDictionary);
        try (FieldVector myAppUseCountryDictionaryEncoded = (FieldVector) DictionaryEncoder
                .encode(myAppUseCountryDictionary, myCountryDictionary);
             VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(myAppUseCountryDictionaryEncoded);
             FileOutputStream fileOutputStream = new FileOutputStream(file);
             ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, provider, fileOutputStream.getChannel())
        ) {
            System.out.println("Data to retain through Dictionary: " +myAppUseCountryDictionaryEncoded);
            writer.start();
            writer.writeBatch();
            writer.end();
            System.out.println("Record batches written: " + writer.getRecordBlocks().size() + ". Number of rows written: " + vectorSchemaRoot.getRowCount());
            try(
                    BufferAllocator rootAllocator = new RootAllocator();
                    FileInputStream fileInputStream = new FileInputStream(file);
                    ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)
            ){
                for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                    reader.loadRecordBatch(arrowBlock);
                    FieldVector myAppUseCountryDictionaryEncodedRead = reader.getVectorSchemaRoot().getVector("app-use-country-dict");
                    Dictionary myAppUseCountryDictionaryRead = reader.getDictionaryVectors().get(123L);
                    System.out.println("Data to retain recovered: " + myAppUseCountryDictionaryEncodedRead);
                    System.out.println("Dictionary recovered: " + myAppUseCountryDictionaryRead);
                    try (ValueVector readVector = DictionaryEncoder.decode(myAppUseCountryDictionaryEncodedRead, myAppUseCountryDictionaryRead)) {
                        System.out.println("Data to retain recovered decoded: " + readVector);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

.. testoutput::

    Dictionary used: Dictionary DictionaryEncoding[id=123,ordered=false,indexType=Int(32, true)] [Andorra, Cuba, Grecia, Guinea, Islandia, Malta, Tailandia, Uganda, Yemen, Zambia]
    Data to retain: [Andorra, Guinea, Islandia, Malta, Uganda]
    Data to retain through Dictionary: [0, 3, 4, 5, 7]
    Record batches written: 1. Number of rows written: 5
    Data to retain recovered: [0, 3, 4, 5, 7]
    Dictionary recovered: Dictionary DictionaryEncoding[id=123,ordered=false,indexType=Int(32, true)] [Andorra, Cuba, Grecia, Guinea, Islandia, Malta, Tailandia, Uganda, Yemen, Zambia]
    Data to retain recovered decoded: [Andorra, Guinea, Islandia, Malta, Uganda]
