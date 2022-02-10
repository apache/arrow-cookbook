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