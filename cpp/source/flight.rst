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

============
Arrow Flight
============

This section contains a number of recipes for working with Arrow
Flight, an RPC library specialized for tabular datasets. For more
about Flight, see :doc:`format/Flight`.

.. contents::

Simple Parquet storage service with Arrow Flight
================================================

We'll implement a service that provides a key-value store for tabular
data, using Flight to handle uploads/requests and Parquet to store the
actual data.

First, we'll implement the service itself. For simplicity, we won't
use the :doc:`Datasets <./datasets>` API in favor of just using the
Parquet API directly.

.. literalinclude:: ../code/flight.cc
   :language: cpp
   :linenos:
   :start-at: class ParquetStorageService
   :end-at: end ParquetStorageService
   :caption: Parquet storage service, server implementation

First, we'll start our server:

.. recipe:: ../code/flight.cc ParquetStorageService::StartServer
   :dedent: 2

We can then create a client and connect to the server:

.. recipe:: ../code/flight.cc ParquetStorageService::Connect
   :dedent: 2

First, we'll create and upload a table, which will get stored in a
Parquet file by the server.

.. recipe:: ../code/flight.cc ParquetStorageService::DoPut
   :dedent: 2

Once we do so, we can retrieve the metadata for that dataset:

.. recipe:: ../code/flight.cc ParquetStorageService::GetFlightInfo
   :dedent: 2

And get the data back:

.. recipe:: ../code/flight.cc ParquetStorageService::DoGet
   :dedent: 2

Then, we'll delete the dataset:

.. recipe:: ../code/flight.cc ParquetStorageService::DoAction
   :dedent: 2

And confirm that it's been deleted:

.. recipe:: ../code/flight.cc ParquetStorageService::ListFlights
   :dedent: 2

Finally, we'll stop our server:

.. recipe:: ../code/flight.cc ParquetStorageService::StopServer
   :dedent: 2
