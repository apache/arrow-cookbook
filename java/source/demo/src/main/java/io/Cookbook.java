package io;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.*;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.*;
import java.nio.channels.Channels;

public class Cookbook {
    public static void main(String[] args) throws Exception {
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE); // deal with byte buffer allocation

        VectorSchemaRoot vectorSchemaRoot = Util.createVectorSchemaRoot();

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
}
