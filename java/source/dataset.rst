=======
Dataset
=======

Current java project that use JNI are:

* Arrow Java Dataset: Java implementation of Arrow Dataset API/Framework - JniLoader INSTANCE = new JniLoader(Collections.singletonList("arrow_cdata_jni"))
* Arrow Java C Data Interface: Java implementation of C Data Interface - JniLoader INSTANCE = new JniLoader(Collections.singletonList("arrow_dataset_jni"))

.. contents::

Schema
======

Let read schema information for a parquet file using arrow dataset module (Number of rows in each file: 3)

Inspect Schema
**************

.. testcode::

    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.util.AutoCloseables;

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
    Schema schema = datasetFactory.inspect();
    AutoCloseables.close(datasetFactory);

    System.out.println(schema);

.. testoutput::

    Schema<id: Int(32, true), name: Utf8>(metadata: {parquet.avro.schema={"type":"record","name":"User","namespace":"org.apache.arrow.dataset","fields":[{"name":"id","type":["int","null"]},{"name":"name","type":["string","null"]}]}, writer.model.name=avro})

Infer Schema
************

.. testcode::

    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.util.AutoCloseables;

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
    ScanOptions options = new ScanOptions(1);
    Dataset dataset = datasetFactory.finish();
    Scanner scanner = dataset.newScan(options);
    Schema schema = scanner.schema();
    AutoCloseables.close(datasetFactory, dataset, scanner);

    System.out.println(schema);

.. testoutput::

    Schema<id: Int(32, true), name: Utf8>(metadata: {parquet.avro.schema={"type":"record","name":"User","namespace":"org.apache.arrow.dataset","fields":[{"name":"id","type":["int","null"]},{"name":"name","type":["string","null"]}]}, writer.model.name=avro})

Query Parquet File
==================

Let query information for a parquet file usign arrow dataset module (Number of rows in each file: 3)

Query Data Size
***************

.. testcode::

    import com.google.common.collect.Streams;
    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;
    import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

    import java.util.List;
    import java.util.stream.Collectors;
    import java.util.stream.StreamSupport;

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
    ScanOptions options = new ScanOptions(/*batchSize*/ 1);
    Dataset dataset = datasetFactory.finish();
    Scanner scanner = dataset.newScan(options);
    List<ArrowRecordBatch> batches = StreamSupport.stream(scanner.scan().spliterator(), false).flatMap(t -> Streams.stream(t.execute())).collect(Collectors.toList());
    AutoCloseables.close(datasetFactory, dataset, scanner);

    System.out.println(batches.size()); // totaRows 3 / batchSize 1 = 3

.. testoutput::

    3

Query Data Content
******************

.. testcode::

    import com.google.common.collect.Streams;
    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;
    import org.apache.arrow.vector.FieldVector;
    import org.apache.arrow.vector.VectorLoader;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
    import org.apache.arrow.vector.types.pojo.Schema;

    import java.util.List;
    import java.util.stream.Collectors;
    import java.util.stream.StreamSupport;

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
    ScanOptions options = new ScanOptions(1);
    Dataset dataset = datasetFactory.finish();
    Scanner scanner = dataset.newScan(options);
    Schema schema = scanner.schema();
    List<ArrowRecordBatch> batches = StreamSupport.stream(scanner.scan().spliterator(), false).flatMap(t -> Streams.stream(t.execute())).collect(Collectors.toList());
    int fieldCount = schema.getFields().size();
    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, rootAllocator)) {
        VectorLoader loader = new VectorLoader(vsr);
        for (ArrowRecordBatch batch : batches) {
            loader.load(batch);
            int batchRowCount = vsr.getRowCount();
            for (int i = 0; i < fieldCount; i++) {
                FieldVector vector = vsr.getVector(i);
                for (int j = 0; j < batchRowCount; j++) {
                    Object object = vector.getObject(j);
                    System.out.println(object);
                }
            }
        }
    }
    AutoCloseables.close(datasetFactory, dataset, scanner);

.. testoutput::

    1
    David
    2
    Gladis
    3
    Juan

Query Data Content with Projection
**********************************

.. testcode::

    import com.google.common.collect.Streams;
    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;
    import org.apache.arrow.vector.FieldVector;
    import org.apache.arrow.vector.VectorLoader;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
    import org.apache.arrow.vector.types.pojo.Schema;

    import java.util.List;
    import java.util.Optional;
    import java.util.stream.Collectors;
    import java.util.stream.StreamSupport;

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
    String[] projection = new String[] {"name"};
    ScanOptions options = new ScanOptions(1, Optional.of(projection));
    Dataset dataset = datasetFactory.finish();
    Scanner scanner = dataset.newScan(options);
    Schema schema = scanner.schema();
    List<ArrowRecordBatch> batches = StreamSupport.stream(scanner.scan().spliterator(), false).flatMap(t -> Streams.stream(t.execute())).collect(Collectors.toList());
    int fieldCount = schema.getFields().size();
    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, rootAllocator)) {
        VectorLoader loader = new VectorLoader(vsr);
        for (ArrowRecordBatch batch : batches) {
            loader.load(batch);
            int batchRowCount = vsr.getRowCount();
            for (int i = 0; i < fieldCount; i++) {
                FieldVector vector = vsr.getVector(i);
                for (int j = 0; j < batchRowCount; j++) {
                    Object object = vector.getObject(j);
                    System.out.println(object);
                }
            }
        }
    }
    AutoCloseables.close(datasetFactory, dataset, scanner);

.. testoutput::

    David
    Gladis
    Juan
