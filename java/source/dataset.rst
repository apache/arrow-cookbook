.. _arrow-dataset:

=======
Dataset
=======

* `Arrow Java Dataset`_: Java implementation of Arrow Datasets library. Implement Dataset Java API by JNI to C++.

.. contents::

Constructing Datasets
=====================

We can construct a dataset with an auto-inferred schema.

.. testcode::

    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.RootAllocator;
    import java.util.stream.StreamSupport;

    try (RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)) {
        String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
        try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri)) {
            try(Dataset dataset = datasetFactory.finish()){
                ScanOptions options = new ScanOptions(/*batchSize*/ 100);
                try(Scanner scanner = dataset.newScan(options)){
                    System.out.println(StreamSupport.stream(scanner.scan().spliterator(), false).count());
                }
            }
        }
    }

.. testoutput::

    1

Let construct our dataset with predefined schema.

.. testcode::

    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.RootAllocator;
    import java.util.stream.StreamSupport;

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
    try (RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)) {
        try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri)) {
            try(Dataset dataset = datasetFactory.finish(datasetFactory.inspect())){
                ScanOptions options = new ScanOptions(/*batchSize*/ 100);
                try(Scanner scanner = dataset.newScan(options)){
                    System.out.println(StreamSupport.stream(scanner.scan().spliterator(), false).count());
                }
            }
        }
    }

.. testoutput::

    1

Getting the Schema
==================

During Dataset Construction
***************************

.. testcode::

    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.types.pojo.Schema;

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
    try(RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)){
        try(DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri)){
            Schema schema = datasetFactory.inspect();

            System.out.println(schema);
        }
    }

.. testoutput::

    Schema<id: Int(32, true), name: Utf8>(metadata: {parquet.avro.schema={"type":"record","name":"User","namespace":"org.apache.arrow.dataset","fields":[{"name":"id","type":["int","null"]},{"name":"name","type":["string","null"]}]}, writer.model.name=avro})

From a Dataset
**************

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

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
    try(RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)){
        try(DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri)){
            ScanOptions options = new ScanOptions(/*batchSize*/ 1);
            try(Dataset dataset = datasetFactory.finish()){
                try(Scanner scanner = dataset.newScan(options)){
                    Schema schema = scanner.schema();

                    System.out.println(schema);
                }
            }
        }
    }

.. testoutput::

    Schema<id: Int(32, true), name: Utf8>(metadata: {parquet.avro.schema={"type":"record","name":"User","namespace":"org.apache.arrow.dataset","fields":[{"name":"id","type":["int","null"]},{"name":"name","type":["string","null"]}]}, writer.model.name=avro})

Query Parquet File
==================

Let query information for a parquet file.

Query Data Content For File
***************************

.. testcode::

    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VectorLoader;
    import org.apache.arrow.vector.VectorSchemaRoot;

    import java.util.stream.Stream;

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
    try(RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
        Dataset dataset = datasetFactory.finish()){
        ScanOptions options = new ScanOptions(/*batchSize*/ 100);
        try(Scanner scanner = dataset.newScan(options);
            VectorSchemaRoot vsr = VectorSchemaRoot.create(scanner.schema(), rootAllocator)){
            scanner.scan().forEach(scanTask-> {
                VectorLoader loader = new VectorLoader(vsr);
                scanTask.execute().forEachRemaining(arrowRecordBatch -> {
                    loader.load(arrowRecordBatch);
                    System.out.print(vsr.contentToTSVString());
                    arrowRecordBatch.close();
                });
            });
        }
    }

.. testoutput::

    id    name
    1    David
    2    Gladis
    3    Juan

Query Data Content For Directory
********************************

Consider that we have these files: data1: 3 rows, data2: 3 rows and data3: 250 rows.

.. testcode::

    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VectorLoader;
    import org.apache.arrow.vector.VectorSchemaRoot;

    import java.util.stream.Stream;

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/";
    try(RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
        Dataset dataset = datasetFactory.finish()){
        ScanOptions options = new ScanOptions(/*batchSize*/ 100);
        try(Scanner scanner = dataset.newScan(options);
            VectorSchemaRoot vsr = VectorSchemaRoot.create(scanner.schema(), rootAllocator)){
            scanner.scan().forEach(scanTask-> {
                VectorLoader loader = new VectorLoader(vsr);
                final int[] count = {1};
                scanTask.execute().forEachRemaining(arrowRecordBatch -> {
                    loader.load(arrowRecordBatch);
                    System.out.println("Batch: " + count[0]++ + ", RowCount: " + vsr.getRowCount());
                    arrowRecordBatch.close();
                });
            });
        }
    }

.. testoutput::

    Batch: 1, RowCount: 3
    Batch: 2, RowCount: 3
    Batch: 3, RowCount: 100
    Batch: 4, RowCount: 100
    Batch: 5, RowCount: 50

Query Data Content with Projection
**********************************

In case we need to project only certain columns we could configure ScanOptions with projections needed.

.. testcode::

    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VectorLoader;
    import org.apache.arrow.vector.VectorSchemaRoot;

    import java.util.Optional;

    String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data1.parquet";
    try(RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
        Dataset dataset = datasetFactory.finish()){
        String[] projection = new String[] {"name"};
        ScanOptions options = new ScanOptions(/*batchSize*/ 100, Optional.of(projection));
        try(Scanner scanner = dataset.newScan(options);
            VectorSchemaRoot vsr = VectorSchemaRoot.create(scanner.schema(), rootAllocator)){
            scanner.scan().forEach(scanTask-> {
                VectorLoader loader = new VectorLoader(vsr);
                scanTask.execute().forEachRemaining(arrowRecordBatch -> {
                    loader.load(arrowRecordBatch);
                    System.out.print(vsr.contentToTSVString());
                    arrowRecordBatch.close();
                });
            });
        }
    }

.. testoutput::

    name
    David
    Gladis
    Juan


.. _Arrow Java Dataset: https://arrow.apache.org/docs/dev/java/dataset.html