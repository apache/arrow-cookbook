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

======================
Creating Arrow Objects
======================

Recipes related to the creation of Arrays, Tables,
Tensors and all other Arrow entities.

.. contents::

Create Arrays from Standard C++
===============================

Typed subclasses of :cpp:class:`arrow::ArrayBuilder` make it easy
to efficiently create Arrow arrays from existing C++ data:

.. recipe:: ../code/creating_arrow_objects.cc CreatingArrays
  :caption: Creating an array from C++ primitives
  :dedent: 2

.. note::

    Builders will allocate data as needed and insertion should
    have constant amortized time.

Builders can also consume standard C++ containers:

.. recipe:: ../code/creating_arrow_objects.cc CreatingArraysPtr
  :dedent: 2

.. note::
    
    Builders will not take ownership of data in containers and will make a
    copy of the underlying data.