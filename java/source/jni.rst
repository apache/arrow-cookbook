===
JNI
===

Recipes related to leveraging Arrow java jni

.. contents::

Current java project that use JNI are:

* Arrow Java Dataset: Java implementation of Arrow Dataset API/Framework - JniLoader INSTANCE = new JniLoader(Collections.singletonList("arrow_cdata_jni"))
* Arrow Java C Data Interface: Java implementation of C Data Interface - JniLoader INSTANCE = new JniLoader(Collections.singletonList("arrow_dataset_jni"))

Arrow Java Dataset
==================

Schema
******

Let read schema information for a parquet file using arrow dataset module (Number of rows in each file: 1000)

Inspect Schema
--------------

.. testcode::

    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.util.AutoCloseables;

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/userdata.parquet";
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
    Schema schema = datasetFactory.inspect();
    AutoCloseables.close(datasetFactory);

    System.out.println(schema);

.. testoutput::

    Schema<registration_dttm: Timestamp(NANOSECOND, null), id: Int(32, true), first_name: Utf8, last_name: Utf8, email: Utf8, gender: Utf8, ip_address: Utf8, cc: Utf8, country: Utf8, birthdate: Utf8, salary: FloatingPoint(DOUBLE), title: Utf8, comments: Utf8>

Infer Schema
------------

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

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/userdata.parquet";
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
    ScanOptions options = new ScanOptions(100);
    Dataset dataset = datasetFactory.finish();
    Scanner scanner = dataset.newScan(options);
    Schema schema = scanner.schema();
    AutoCloseables.close(datasetFactory, dataset, scanner);

    System.out.println(schema);

.. testoutput::

    Schema<registration_dttm: Timestamp(NANOSECOND, null), id: Int(32, true), first_name: Utf8, last_name: Utf8, email: Utf8, gender: Utf8, ip_address: Utf8, cc: Utf8, country: Utf8, birthdate: Utf8, salary: FloatingPoint(DOUBLE), title: Utf8, comments: Utf8>

Query Parquet File
******************

Let query information for a parquet file usign arrow dataset module (Number of rows in each file: 1000)

Query Data Size
---------------

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

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/userdata.parquet";
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
    ScanOptions options = new ScanOptions(/*batchSize*/ 5);
    Dataset dataset = datasetFactory.finish();
    Scanner scanner = dataset.newScan(options);
    List<ArrowRecordBatch> batches = StreamSupport.stream(scanner.scan().spliterator(), false).flatMap(t -> Streams.stream(t.execute())).collect(Collectors.toList());
    AutoCloseables.close(datasetFactory, dataset, scanner);

    System.out.println(batches.size()); // totaRows 1000 / batchSize 5 = 200

.. testoutput::

    200

Query Data Content
------------------

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

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/userdata.parquet";
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
    ScanOptions options = new ScanOptions(/*batchSize*/ 5);
    Dataset dataset = datasetFactory.finish();
    Scanner scanner = dataset.newScan(options);
    List<ArrowRecordBatch> batches = StreamSupport.stream(scanner.scan().spliterator(), false).flatMap(t -> Streams.stream(t.execute())).collect(Collectors.toList());
    AutoCloseables.close(datasetFactory, dataset, scanner);

    System.out.println(batches.size()); // totaRows 1000 / batchSize 5 = 200

.. testoutput::

    200

Query Data Content with Projection
----------------------------------

