import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.*;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class Io {
    public static void main(String[] args) throws IOException {
        // create a column data type
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

        // create a definition
        Schema schemaPerson = new Schema(asList(name, document, age, points));

        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE); // deal with byte buffer allocation
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);

        // getting field vectors
        VarCharVector nameVectorOption1 = (VarCharVector) vectorSchemaRoot.getVector("name"); //interface FieldVector
        VarCharVector documentVectorOption1 = (VarCharVector) vectorSchemaRoot.getVector("document"); //interface FieldVector
        IntVector ageVectorOption1 = (IntVector) vectorSchemaRoot.getVector("age");
        ListVector pointsVectorOption1 = (ListVector) vectorSchemaRoot.getVector("points");

        // add values to the field vectors
        setVector(nameVectorOption1, "david".getBytes(), "gladis".getBytes(), "juan".getBytes());
        setVector(documentVectorOption1, "A".getBytes(), "B".getBytes(), "C".getBytes());
        setVector(ageVectorOption1, 10,20,30);
        setVector(pointsVectorOption1, asList(1,3,5,7,9), asList(2,4,6,8,10), asList(1,2,3,5,8));
        vectorSchemaRoot.setRowCount(3);
        String initialContentToValidate = vectorSchemaRoot.contentToTSVString();
        System.out.println(initialContentToValidate);

        // random access format
        // write - Random access to file
        File file = new File("randon_access.arrow");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel());
        writer.start();
        writer.writeBatch();
        writer.end();

        // write - Random access to buffer
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowFileWriter writerBuffer = new ArrowFileWriter(vectorSchemaRoot, null, Channels.newChannel(out));
        writerBuffer.start();
        writerBuffer.writeBatch();
        writerBuffer.end();

        // read - Random access to file
        FileInputStream fileInputStream = new FileInputStream(file);
        ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator);
        reader.loadNextBatch();
        VectorSchemaRoot vectorSchemaRootReaded = reader.getVectorSchemaRoot();
        if(initialContentToValidate.equals(vectorSchemaRootReaded.contentToTSVString())){
            System.out.println("Initial=ReadRandomAccessFile");
        }

        // read - Random access to buffer
        ArrowFileReader readerBuffer = new ArrowFileReader(new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(out.toByteArray())), rootAllocator);
        readerBuffer.loadNextBatch();
        VectorSchemaRoot vectorSchemaRootRandomReadedFromBuffer = readerBuffer.getVectorSchemaRoot();
        if(initialContentToValidate.equals(vectorSchemaRootRandomReadedFromBuffer.contentToTSVString())){
            System.out.println("Initial=ReadRandomAccessBuffer");
        }

        // streaming format
        // write - Streaming to file
        File fileStream = new File("streaming.arrow");
        FileOutputStream fileOutputStreamforStream = new FileOutputStream(fileStream);
        ArrowStreamWriter writerStream = new ArrowStreamWriter(vectorSchemaRoot, null, fileOutputStreamforStream);
        writerStream.start();
        writerStream.writeBatch();
        writerStream.end();

        // write - Streaming to buffer
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        ArrowStreamWriter writerStreamBuffer = new ArrowStreamWriter(vectorSchemaRoot, null, outBuffer);
        writerStreamBuffer.start();
        writerStreamBuffer.writeBatch();
        writerStreamBuffer.end();

        // read -Streaming to file
        FileInputStream fileInputStreamForStream = new FileInputStream(fileStream);
        ArrowStreamReader readerStream = new ArrowStreamReader(fileInputStreamForStream, rootAllocator);
        readerStream.loadNextBatch();
        VectorSchemaRoot vectorSchemaRootReadedForStream = readerStream.getVectorSchemaRoot();
        if(initialContentToValidate.equals(vectorSchemaRootReadedForStream.contentToTSVString())){
            System.out.println("Initial=ReadStreamingFile");
        }

        // read - Streaming to buffer
        ArrowStreamReader readerBufferForStream = new ArrowStreamReader(new ByteArrayInputStream(outBuffer.toByteArray()), rootAllocator);
        readerBufferForStream.loadNextBatch();
        VectorSchemaRoot vectorSchemaRootStreamingReadedFromBuffer = readerBufferForStream.getVectorSchemaRoot();
        if(initialContentToValidate.equals(vectorSchemaRootStreamingReadedFromBuffer.contentToTSVString())){
            System.out.println("Initial=ReadStreamingBuffer");
        }
    }

    public static void setVector(IntVector vector, Integer... values) {
        final int length = values.length;
        vector.allocateNew(length);
        for (int i = 0; i < length; i++) {
            if (values[i] != null) {
                vector.set(i, values[i]);
            }
        }
        vector.setValueCount(length);
    }

    public static void setVector(VarCharVector vector, byte[]... values) {
        final int length = values.length;
        vector.allocateNewSafe();
        for (int i = 0; i < length; i++) {
            if (values[i] != null) {
                vector.set(i, values[i]);
            }
        }
        vector.setValueCount(length);
    }

    public static void setVector(ListVector vector, List<Integer>... values) {
        vector.allocateNewSafe();
        Types.MinorType type = Types.MinorType.INT;
        vector.addOrGetVector(FieldType.nullable(type.getType()));

        IntVector dataVector = (IntVector) vector.getDataVector();
        dataVector.allocateNew();

        // set underlying vectors
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

}
