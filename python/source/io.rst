========================
Reading and Writing Data
========================

Recipes related to reading and writing data from disk using
Apache Arrow.

.. contents::

.. testsetup::

    import pyarrow as pa

Write a Parquet file
====================

Given an array with 100 numbers, from 0 to 99

.. testcode::

    import numpy as np
    import pyarrow as pa

    arr = pa.array(np.arange(100))

    print(f"{arr[0]} .. {arr[-1]}")

.. testoutput::

    0 .. 99

To write it to a Parquet file, 
as Parquet is a format that contains multiple named columns,
we must create a :class:`pyarrow.Table` out of it,
so that we get a table of a single column which can then be
written to a Parquet file. 

.. testcode::

    table = pa.Table.from_arrays([arr], names=["col1"])

Once we have a table, it can be written to a Parquet File 
using the functions provided by the ``pyarrow.parquet`` module

.. testcode::

    import pyarrow.parquet as pq

    pq.write_table(table, "example.parquet", compression=None)

Reading a Parquet file
======================

Given a Parquet file, it can be read back to a :class:`pyarrow.Table`
by using :func:`pyarrow.parquet.read_table` function

.. testcode::

    import pyarrow.parquet as pq

    table = pq.read_table("example.parquet")

The resulting table will contain the same columns that existed in
the parquet file as :class:`ChunkedArray`

.. testcode::

    print(table)

    col1 = table["col1"]
    print(f"{type(col1).__name__} = {col1[0]} .. {col1[-1]}")

.. testoutput::

    pyarrow.Table
    col1: int64
    ChunkedArray = 0 .. 99

Reading a subset of Parquet data
================================

When reading a Parquet file with :func:`pyarrow.parquet.read_table` 
it is possible to restrict which Columns and Rows will be read
into memory by using the ``filters`` and ``columns`` arguments

.. testcode::

    import pyarrow.parquet as pq

    table = pq.read_table("example.parquet", 
                          columns=["col1"],
                          filters=[
                              ("col1", ">", 5),
                              ("col1", "<", 10),
                          ])

The resulting table will contain only the projected columns
and filtered rows. Refer to :func:`pyarrow.parquet.read_table`
documentation for details about the syntax for filters.

.. testcode::

    print(table)

    col1 = table["col1"]
    print(f"{type(col1).__name__} = {col1[0]} .. {col1[-1]}")

.. testoutput::

    pyarrow.Table
    col1: int64
    ChunkedArray = 6 .. 9
    

Saving Arrow Arrays to disk
===========================

Apart from using arrow to read and save common file formats like Parquet,
it is possible to dump data in the raw arrow format which allows 
direct memory mapping of data from disk. This format is called
the Arrow IPC format.

Given an array with 100 numbers, from 0 to 99

.. testcode::

    import numpy as np
    import pyarrow as pa

    arr = pa.array(np.arange(100))

    print(f"{arr[0]} .. {arr[-1]}")

.. testoutput::

    0 .. 99

We can save the array by making a :class:`pyarrow.RecordBatch` out
of it and writing the record batch to disk.

.. testcode::

    schema = pa.schema([
        pa.field('nums', arr.type)
    ])

    with pa.OSFile('arraydata.arrow', 'wb') as sink:
        with pa.ipc.new_file(sink, schema=schema) as writer:
            batch = pa.record_batch([arr], schema=schema)
            writer.write(batch)

If we were to save multiple arrays into the same file,
we would just have to adapt the ``schema`` accordingly and add
them all to the ``record_batch`` call.

Memory Mapping Arrow Arrays from disk
=====================================

Arrow arrays that have been written to disk in the Arrow IPC
format can be memory mapped back directly from the disk.

.. testcode::

    with pa.memory_map('arraydata.arrow', 'r') as source:
        loaded_arrays = pa.ipc.open_file(source).read_all()

.. testcode::

    arr = loaded_arrays[0]
    print(f"{arr[0]} .. {arr[-1]}")

.. testoutput::

    0 .. 99

Writing CSV files
=================

It is possible to write an Arrow :class:`pyarrow.Table` to
a CSV file using the :func:`pyarrow.csv.write_csv` function

.. testcode::

    arr = pa.array(range(100))
    table = pa.Table.from_arrays([arr], names=["col1"])
    
    import pyarrow.csv
    pa.csv.write_csv(table, "table.csv",
                     write_options=pa.csv.WriteOptions(include_header=True))

Writing CSV files incrementally
===============================

If you need to write data to a CSV file incrementally
as you generate or retrieve the data and you don't want to keep
in memory the whole table to write it at once, it's possible to use
:class:`pyarrow.csv.CSVWriter` to write data incrementally

.. testcode::

    schema = pa.schema([("col1", pa.int32())])
    with pa.csv.CSVWriter("table.csv", schema=schema) as writer:
        for chunk in range(10):
            datachunk = range(chunk*10, (chunk+1)*10)
            table = pa.Table.from_arrays([pa.array(datachunk)], schema=schema)
            writer.write(table)

It's equally possible to write :class:`pyarrow.RecordBatch`
by passing them as you would for tables.

Reading CSV files
=================

Arrow can read :class:`pyarrow.Table` entities from CSV using an
optimized codepath that can leverage multiple threads.

.. testcode::

    import pyarrow.csv

    table = pa.csv.read_csv("table.csv")

Arrow will do its best to infer data types.  Further options can be
provided to :func:`pyarrow.csv.read_csv` to drive
:class:`pyarrow.csv.ConvertOptions`.

.. testcode::

    print(table)

    col1 = table["col1"]
    print(f"{type(col1).__name__} = {col1[0]} .. {col1[-1]}")

.. testoutput::

    pyarrow.Table
    col1: int64
    ChunkedArray = 0 .. 99

Writing Partitioned Datasets 
============================

When your dataset is big it usually makes sense to split it into
multiple separate files. You can do this manually or use 
:func:`pyarrow.dataset.write_dataset` to let Arrow do the effort
of splitting the data in chunks for you.

The ``partitioning`` argument allows to tell :func:`pyarrow.dataset.write_dataset`
for which columns the data should be split. 

For example given 100 birthdays, within 2000 and 2009

.. testcode::

    import numpy.random
    data = pa.table({"day": numpy.random.randint(1, 31, size=100), 
                     "month": numpy.random.randint(1, 12, size=100),
                     "year": [2000 + x // 10 for x in range(100)]})

Then we could partition the data by the year column so that it
gets saved in 10 different files:

.. testcode::

    import pyarrow as pa
    import pyarrow.dataset as ds

    ds.write_dataset(data, "./partitioned", format="parquet",
                     partitioning=ds.partitioning(pa.schema([("year", pa.int16())])))

Arrow will partition datasets in subdirectories by default, which will
result in 10 different directories named with the value of the partitioning
column each with a file containing the subset of the data for that partition:

.. testcode::

    from pyarrow import fs

    localfs = fs.LocalFileSystem()
    partitioned_dir_content = localfs.get_file_info(fs.FileSelector("./partitioned", recursive=True))
    files = sorted((f.path for f in partitioned_dir_content if f.type == fs.FileType.File))

    for file in files:
        print(file)

.. testoutput::

    ./partitioned/2000/part-0.parquet
    ./partitioned/2001/part-1.parquet
    ./partitioned/2002/part-2.parquet
    ./partitioned/2003/part-3.parquet
    ./partitioned/2004/part-4.parquet
    ./partitioned/2005/part-6.parquet
    ./partitioned/2006/part-5.parquet
    ./partitioned/2007/part-7.parquet
    ./partitioned/2008/part-8.parquet
    ./partitioned/2009/part-9.parquet

Reading Partitioned data
========================

In some cases, your dataset might be composed by multiple separate
files each containing a piece of the data. 

.. testsetup::

    import pathlib
    import pyarrow.parquet as pq

    examples = pathlib.Path("examples")
    examples.mkdir(exist_ok=True)

    pq.write_table(pa.table({"col1": range(10)}), 
                   examples / "dataset1.parquet", compression=None)
    pq.write_table(pa.table({"col1": range(10, 20)}), 
                   examples / "dataset2.parquet", compression=None)
    pq.write_table(pa.table({"col1": range(20, 30)}), 
                   examples / "dataset3.parquet", compression=None)

In this case the :func:`pyarrow.dataset.dataset` function provides
an interface to discover and read all those files as a single big dataset.

For example if we have a structure like:

.. code-block::

    examples/
    ├── dataset1.parquet
    ├── dataset2.parquet
    └── dataset3.parquet

Then, pointing the :func:`pyarrow.dataset.dataset` function to the ``examples`` directory
will discover those parquet files and will expose them all as a single
:class:`pyarrow.dataset.Dataset`:

.. testcode::

    import pyarrow.dataset as ds

    dataset = ds.dataset("./examples", format="parquet")
    print(dataset.files)

.. testoutput::

    ['./examples/dataset1.parquet', './examples/dataset2.parquet', './examples/dataset3.parquet']

The whole dataset can be viewed as a single big table using
:meth:`pyarrow.dataset.Dataset.to_table`. While each parquet file
contains only 10 rows, converting the dataset to a table will
expose them as a single Table.

.. testcode::

    table = dataset.to_table()
    print(table)

    col1 = table["col1"]
    print(f"{type(col1).__name__} = {col1[0]} .. {col1[-1]}")

.. testoutput::

    pyarrow.Table
    col1: int64
    ChunkedArray = 0 .. 29

Notice that converting to a table will force all data to be loaded 
in memory.  For big datasets is usually not what you want.

For this reason, it might be better to rely on the 
:meth:`pyarrow.dataset.Dataset.to_batches` method, which will
iteratively load the dataset one chunk of data at the time returning a 
:class:`pyarrow.RecordBatch` for each one of them.

.. testcode::

    for record_batch in dataset.to_batches():
        col1 = record_batch.column("col1")
        print(f"{col1._name} = {col1[0]} .. {col1[-1]}")

.. testoutput::

    col1 = 0 .. 9
    col1 = 10 .. 19
    col1 = 20 .. 29

Reading Partitioned Data from S3
================================

The :class:`pyarrow.dataset.Dataset` is also able to abstract
partitioned data coming from remote sources like S3 or HDFS.

.. testcode::

    from pyarrow import fs

    # List content of s3://ursa-labs-taxi-data/2011
    s3 = fs.SubTreeFileSystem(
        "ursa-labs-taxi-data", 
        fs.S3FileSystem(region="us-east-2", anonymous=True)
    )
    for entry in s3.get_file_info(fs.FileSelector("2011", recursive=True)):
        if entry.type == fs.FileType.File:
            print(entry.path)

.. testoutput::

    2011/01/data.parquet
    2011/02/data.parquet
    2011/03/data.parquet
    2011/04/data.parquet
    2011/05/data.parquet
    2011/06/data.parquet
    2011/07/data.parquet
    2011/08/data.parquet
    2011/09/data.parquet
    2011/10/data.parquet
    2011/11/data.parquet
    2011/12/data.parquet

The data in the bucket can be loaded as a single big dataset partitioned
by ``month`` using

.. testcode::

    dataset = ds.dataset("s3://ursa-labs-taxi-data/2011",
                         partitioning=["month"])
    for f in dataset.files[:10]:
        print(f)
    print("...")

.. testoutput::

    ursa-labs-taxi-data/2011/01/data.parquet
    ursa-labs-taxi-data/2011/02/data.parquet
    ursa-labs-taxi-data/2011/03/data.parquet
    ursa-labs-taxi-data/2011/04/data.parquet
    ursa-labs-taxi-data/2011/05/data.parquet
    ursa-labs-taxi-data/2011/06/data.parquet
    ursa-labs-taxi-data/2011/07/data.parquet
    ursa-labs-taxi-data/2011/08/data.parquet
    ursa-labs-taxi-data/2011/09/data.parquet
    ursa-labs-taxi-data/2011/10/data.parquet
    ...

The dataset can then be used with :meth:`pyarrow.dataset.Dataset.to_table`
or :meth:`pyarrow.dataset.Dataset.to_batches` like you would for a local one.

.. note::

    It is possible to load partitioned data also in the ipc arrow
    format or in feather format.

.. warning::

    If the above code throws an error most likely the reason is your
    AWS credentials are not set. Follow these instructions to get
    ``AWS Access Key Id`` and ``AWS Secret Access Key``: 
    `AWS Credentials <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html>`_.

    The credentials are normally stored in ``~/.aws/credentials`` (on Mac or Linux)
    or in ``C:\Users\<USERNAME>\.aws\credentials`` (on Windows) file. 
    You will need to either create or update this file in the appropriate location.

    The contents of the file should look like this:

    .. code-block:: bash 

        [default]
        aws_access_key_id=<YOUR_AWS_ACCESS_KEY_ID>
        aws_secret_access_key=<YOUR_AWS_SECRET_ACCESS_KEY>



Write a Feather file
====================

.. testsetup::

    import numpy as np
    import pyarrow as pa

    arr = pa.array(np.arange(100))

Given an array with 100 numbers, from 0 to 99

.. testcode::

    import numpy as np
    import pyarrow as pa

    arr = pa.array(np.arange(100))

    print(f"{arr[0]} .. {arr[-1]}")

.. testoutput::

    0 .. 99

To write it to a Feather file, as Feather stores multiple columns,
we must create a :class:`pyarrow.Table` out of it,
so that we get a table of a single column which can then be
written to a Feather file. 

.. testcode::

    table = pa.Table.from_arrays([arr], names=["col1"])

Once we have a table, it can be written to a Feather File 
using the functions provided by the ``pyarrow.feather`` module

.. testcode::

    import pyarrow.feather as ft
    
    ft.write_feather(table, 'example.feather')

Reading a Feather file
======================

Given a Feather file, it can be read back to a :class:`pyarrow.Table`
by using :func:`pyarrow.feather.read_table` function

.. testcode::

    import pyarrow.feather as ft

    table = ft.read_table("example.feather")

The resulting table will contain the same columns that existed in
the parquet file as :class:`ChunkedArray`

.. testcode::

    print(table)

    col1 = table["col1"]
    print(f"{type(col1).__name__} = {col1[0]} .. {col1[-1]}")

.. testoutput::

    pyarrow.Table
    col1: int64
    ChunkedArray = 0 .. 99

Reading Line Delimited JSON
===========================

Arrow has builtin support for line-delimited JSON.
Each line represents a row of data as a JSON object.

Given some data in a file where each line is a JSON object
containing a row of data:

.. testcode::

    import tempfile

    with tempfile.NamedTemporaryFile(delete=False, mode="w+") as f:
        f.write('{"a": 1, "b": 2.0, "c": 1}\n')
        f.write('{"a": 3, "b": 3.0, "c": 2}\n')
        f.write('{"a": 5, "b": 4.0, "c": 3}\n')
        f.write('{"a": 7, "b": 5.0, "c": 4}\n')

The content of the file can be read back to a :class:`pyarrow.Table` using
:func:`pyarrow.json.read_json`:

.. testcode::

    import pyarrow as pa
    import pyarrow.json

    table = pa.json.read_json(f.name)

.. testcode::

    print(table.to_pydict())

.. testoutput::

    {'a': [1, 3, 5, 7], 'b': [2.0, 3.0, 4.0, 5.0], 'c': [1, 2, 3, 4]}