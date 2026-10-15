.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

============================
Reading and Writing Datasets
============================

This section contains a number of recipes for reading and writing
datasets.  Datasets are a collection of one or more files containing
tabular data.

.. contents::

Read a Partitioned Dataset
==========================

The individual data files that make up a dataset will often be
distributed across several different directories according to some
kind of partitioning scheme.

This simplifies management of the data and also allows for partial
reads of the dataset by inspecting the file paths and utilizing the
guarantees provided by the partitioning scheme.

This recipe demonstrates the basics of reading a partitioned dataset.
First let us inspect our data:

.. recipe:: ../code/datasets.cc ListPartitionedDataset
  :caption: A listing of files in our dataset
  :dedent: 2

.. note::

    This partitioning scheme of key=value is referred to as "hive"
    partitioning within Arrow.

Now that we have a filesystem and a selector we can go ahead and create
a dataset.  To do this we need to pick a format and a partitioning
scheme.  Once we have all of the pieces we need we can create an 
arrow::dataset::Dataset instance.

.. recipe:: ../code/datasets.cc CreatingADataset
  :caption: Creating an arrow::dataset::Dataset instance
  :dedent: 2

Once we have a dataset object we can read in the data.  Reading the data
from a dataset is sometimes called "scanning" the dataset and the object
we use to do this is an arrow::dataset::Scanner.  The following snippet
shows how to scan the entire dataset into an in-memory table:

.. recipe:: ../code/datasets.cc ScanningADataset
  :caption: Scanning a dataset into an arrow::Table
  :dedent: 2
