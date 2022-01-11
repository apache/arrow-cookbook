.. _arrow-io:

========================
Reading and writing data
========================

Recipes related to reading and writing data from disk using Apache Arrow.

Arrow defines two types of binary formats for serializing record batches `IPC <https://arrow.apache.org/docs/java/ipc.html>`_: Streaming format / File or Random Access format

Arrow vectors can be serialized to disk as the Arrow IPC format. Such files can be directly memory-mapped when read.

.. contents::

Writing Array
=============

Create and populate a VectorSchemaRoot:

.. code-block:: java
   :emphasize-lines: 114

   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.BitVectorHelper;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.vector.VarCharVector;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
   import org.apache.arrow.vector.complex.ListVector;
   import org.apache.arrow.vector.types.Types;
   import org.apache.arrow.vector.types.pojo.ArrowType;
   import org.apache.arrow.vector.types.pojo.Field;
   import org.apache.arrow.vector.types.pojo.FieldType;
   import org.apache.arrow.vector.types.pojo.Schema;
   import java.util.ArrayList;
   import java.util.HashMap;
   import java.util.List;
   import java.util.Map;
   import static java.util.Arrays.asList;

   void setVector(IntVector vector, Integer... values) {
       final int length = values.length;
       vector.allocateNew(length);
       for (int i = 0; i < length; i++) {
           if (values[i] != null) {
               vector.set(i, values[i]);
           }
       }
       vector.setValueCount(length);
   }

   void setVector(VarCharVector vector, byte[]... values) {
       final int length = values.length;
       vector.allocateNewSafe();
       for (int i = 0; i < length; i++) {
           if (values[i] != null) {
               vector.set(i, values[i]);
           }
       }
       vector.setValueCount(length);
   }

   void setVector(ListVector vector, List<Integer>... values) {
       vector.allocateNewSafe();
       Types.MinorType type = Types.MinorType.INT;
       vector.addOrGetVector(FieldType.nullable(type.getType()));

       IntVector dataVector = (IntVector) vector.getDataVector();
       dataVector.allocateNew();

       int curPos = 0;
       vector.getOffsetBuffer().setInt(0, curPos);
       for (int i = 0; i < values.length; i++) {
           if (values[i] == null) {
               BitVectorHelper.unsetBit(vector.getValidityBuffer(), i);
           } else {
               BitVectorHelper.setBit(vector.getValidityBuffer(), i);
               for (int value : values[i]) {
                   dataVector.setSafe(curPos, value);
                   curPos += 1;
               }
           }
           vector.getOffsetBuffer().setInt((i + 1) * BaseRepeatedValueVector.OFFSET_WIDTH, curPos);
       }
       dataVector.setValueCount(curPos);
       vector.setLastSet(values.length - 1);
       vector.setValueCount(values.length);
   }

   VectorSchemaRoot createVectorSchemaRoot(){
       Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);

       Map<String, String> metadata = new HashMap<>();
       metadata.put("A", "Id card");
       metadata.put("B", "Passport");
       metadata.put("C", "Visa");
       Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), null, metadata), null);

       Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

       FieldType intType = new FieldType(true, new ArrowType.Int(32, true), /*dictionary=*/null);
       FieldType listType = new FieldType(true, new ArrowType.List(), /*dictionary=*/null);
       Field childField = new Field("intCol", intType, null);
       List<Field> childFields = new ArrayList<>();
       childFields.add(childField);
       Field points = new Field("points", listType, childFields);

       Schema schemaPerson = new Schema(asList(name, document, age, points));

       RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

       VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);

       VarCharVector nameVectorOption1 = (VarCharVector) vectorSchemaRoot.getVector("name");
       VarCharVector documentVectorOption1 = (VarCharVector) vectorSchemaRoot.getVector("document");
       IntVector ageVectorOption1 = (IntVector) vectorSchemaRoot.getVector("age");
       ListVector pointsVectorOption1 = (ListVector) vectorSchemaRoot.getVector("points");

       setVector(nameVectorOption1, "david".getBytes(), "gladis".getBytes(), "juan".getBytes());
       setVector(documentVectorOption1, "A".getBytes(), "B".getBytes(), "C".getBytes());
       setVector(ageVectorOption1, 10,20,30);
       setVector(pointsVectorOption1, asList(1,3,5,7,9), asList(2,4,6,8,10), asList(1,2,3,5,8));
       vectorSchemaRoot.setRowCount(3);

       return vectorSchemaRoot;
   }

   RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

   VectorSchemaRoot vectorSchemaRoot = createVectorSchemaRoot();

.. code-block:: java
   :emphasize-lines: 1

   jshell> System.out.println(vectorSchemaRoot.contentToTSVString())

   name     document age   points
   david    A        10    [1,3,5,7,9]
   gladis   B        20    [2,4,6,8,10]
   juan     C        30    [1,2,3,5,8]

Writing Arrays with the IPC File Format
***************************************

Write - Random Access to File
-----------------------------

.. code-block:: java
   :emphasize-lines: 9

   import org.apache.arrow.vector.ipc.ArrowFileWriter;
   import java.io.File;
   import java.io.FileOutputStream;

   File file = new File("randon_access.arrow");
   FileOutputStream fileOutputStream = new FileOutputStream(file);
   ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel());
   writer.start();
   writer.writeBatch();
   writer.end();

Write - Random Access to Buffer
-------------------------------

.. code-block:: java
   :emphasize-lines: 8

   import org.apache.arrow.vector.ipc.ArrowFileWriter;
   import java.io.ByteArrayOutputStream;
   import java.nio.channels.Channels;

   ByteArrayOutputStream out = new ByteArrayOutputStream();
   ArrowFileWriter writerBuffer = new ArrowFileWriter(vectorSchemaRoot, null, Channels.newChannel(out));
   writerBuffer.start();
   writerBuffer.writeBatch();
   writerBuffer.end();

Writing Arrays with the IPC Streamed Format
*******************************************

Write - Streaming to File
-------------------------

.. code-block:: java
   :emphasize-lines: 9

   import org.apache.arrow.vector.ipc.ArrowStreamWriter;
   import java.io.File;
   import java.io.FileOutputStream;

   File fileStream = new File("streaming.arrow");
   FileOutputStream fileOutputStreamforStream = new FileOutputStream(fileStream);
   ArrowStreamWriter writerStream = new ArrowStreamWriter(vectorSchemaRoot, null, fileOutputStreamforStream);
   writerStream.start();
   writerStream.writeBatch();
   writerStream.end();

Write - Streaming to Buffer
---------------------------

.. code-block:: java
   :emphasize-lines: 8

   import org.apache.arrow.vector.ipc.ArrowStreamWriter;
   import java.io.ByteArrayOutputStream;

   ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
   ArrowStreamWriter writerStreamBuffer = new ArrowStreamWriter(vectorSchemaRoot, null, outBuffer);
   writerStreamBuffer.start();
   writerStreamBuffer.writeBatch();
   writerStreamBuffer.end();

Read array
==========

Read Arrays with the IPC File Format
************************************

Read - Random Access to File
----------------------------

Consider: Before to run next code you need to write array to file with `Write - random access to file`_.

.. code-block:: java
   :emphasize-lines: 7

   import org.apache.arrow.vector.ipc.ArrowFileReader;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import java.io.FileInputStream;

   FileInputStream fileInputStream = new FileInputStream(file);
   ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator);
   reader.loadNextBatch();
   VectorSchemaRoot vectorSchemaRootReaded = reader.getVectorSchemaRoot();

.. code-block:: java
   :emphasize-lines: 1


   jshell> System.out.println(vectorSchemaRootReaded.contentToTSVString())

   name     document age   points
   david    A        10    [1,3,5,7,9]
   gladis   B        20    [2,4,6,8,10]
   juan     C        30    [1,2,3,5,8]

Read - Random Acces to Buffer
-----------------------------

Consider: Before to run next code you need to write array to file with `Write - random access to buffer`_.

.. code-block:: java
   :emphasize-lines: 7

   import org.apache.arrow.vector.ipc.ArrowFileReader;
   import org.apache.arrow.vector.ipc.SeekableReadChannel;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

   ArrowFileReader readerBuffer = new ArrowFileReader(new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(out.toByteArray())), rootAllocator);
   readerBuffer.loadNextBatch();
   VectorSchemaRoot vectorSchemaRootRandomReadedFromBuffer = readerBuffer.getVectorSchemaRoot();

.. code-block:: java
   :emphasize-lines: 1

   jshell> System.out.println(vectorSchemaRootRandomReadedFromBuffer.contentToTSVString())

   name     document age   points
   david    A        10    [1,3,5,7,9]
   gladis   B        20    [2,4,6,8,10]
   juan     C        30    [1,2,3,5,8]

Read Arrays with the IPC Streamed Format
****************************************

Read - Streaming to File
------------------------

Consider: Before to run next code you need to write array to file with `Write - streaming to file`_.

.. code-block:: java
   :emphasize-lines: 7

   import org.apache.arrow.vector.ipc.ArrowStreamReader;
   import java.io.FileInputStream;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import java.io.FileInputStream;

   FileInputStream fileInputStreamForStream = new FileInputStream(fileStream);
   ArrowStreamReader readerStream = new ArrowStreamReader(fileInputStreamForStream, rootAllocator);
   readerStream.loadNextBatch();
   VectorSchemaRoot vectorSchemaRootReadedForStream = readerStream.getVectorSchemaRoot();

.. code-block:: java
   :emphasize-lines: 1

   jshell> System.out.println(vectorSchemaRootReadedForStream.contentToTSVString())

   name     document age   points
   david    A        10    [1,3,5,7,9]
   gladis   B        20    [2,4,6,8,10]
   juan     C        30    [1,2,3,5,8]

Read - Streaming to Buffer
--------------------------

Consider: Before to run next code you need to write array to file with `Write - streaming to buffer`_.

.. code-block:: java
   :emphasize-lines: 6

   import org.apache.arrow.vector.ipc.ArrowStreamReader;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import java.io.ByteArrayInputStream;

   ArrowStreamReader readerBufferForStream = new ArrowStreamReader(new ByteArrayInputStream(outBuffer.toByteArray()), rootAllocator);
   readerBufferForStream.loadNextBatch();
   VectorSchemaRoot vectorSchemaRootStreamingReadedFromBuffer = readerBufferForStream.getVectorSchemaRoot();

.. code-block:: java
   :emphasize-lines: 1

   jshell> System.out.println(vectorSchemaRootStreamingReadedFromBuffer.contentToTSVString())

   name     document age   points
   david    A        10    [1,3,5,7,9]
   gladis   B        20    [2,4,6,8,10]
   juan     C        30    [1,2,3,5,8]