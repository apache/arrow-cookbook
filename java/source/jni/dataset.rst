=======
Dataset
=======

Current java project that use JNI are:

* `Arrow Java Dataset <https://arrow.apache.org/docs/dev/java/dataset.html>`_: Java implementation of Arrow Dataset API/Framework. JniLoader [arrow_cdata_jni]
* `Arrow Java C Data Interface <https://arrow.apache.org/docs/format/CDataInterface.html>`_: Java implementation of C Data Interface. JniLoader [arrow_dataset_jni]

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
