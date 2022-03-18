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

===================================
Working with the C++ Implementation
===================================

This section of the cookbook goes over basic concepts
that will be needed regardless of how you intend to use
the Arrow C++ implementation.

.. contents::

Working with Status and Result
==============================

C++ libraries often have to choose between throwing exceptions and
returning error codes.  Arrow chooses to return Status and Result
objects as a middle ground.  This makes it clear when a function
can fail and is easier to use than integer arrow codes.

It is important to always check the value of a returned Status object to
ensure that the operation succeeded.  However, this can quickly become
tedious:

.. recipe:: ../code/basic_arrow.cc ReturnNotOkNoMacro
  :caption: Checking the status of every function manually
  :dedent: 2

The macro :c:macro:`ARROW_RETURN_NOT_OK` will take care of some of this
boilerplate for you.  It will run the contained expression and check the resulting
``Status`` or ``Result`` object.  If it failed then it will return the failure.

.. recipe:: ../code/basic_arrow.cc ReturnNotOk
  :caption: Using ARROW_RETURN_NOT_OK to check the status
  :dedent: 2


Using the Visitor Pattern
=========================

Arrow classes DataType, Scalar, and Array have specialized subclasses for
each Arrow type. In order to specialize logic for each subclass, you the visitor
pattern. Arrow provides the macros:

 * `arrow::VisitTypeInline`
 * `arrow::VisitScalarInline`
 * `arrow::VisitArrayInline`

To implement a TypeVisitor we have to implement a Visit method for every possible
DataType we wish to handle. Fortunately, we can often use templates and type
traits to make this less verbose.

Generate Random Data for Given Schema
-------------------------------------

To generate random data for a given schema, a type visitor is helpful.


.. literalinclude:: ../code/basic_arrow.cc
   :language: cpp
   :linenos:
   :start-at: class RandomBatchGenerator
   :end-at: };  // RandomBatchGenerator
   :caption: Using visitor pattern to generate random record batches
  

.. recipe:: ../code/basic_arrow.cc GenerateRandomData
   :dedent: 2


Convert Arbitrary Scalars to Variants
-------------------------------------

.. TODO: Implement converter from rows to values


Generalize Computations Across Arrow Types
------------------------------------------


.. literalinclude:: ../code/basic_arrow.cc
   :language: cpp
   :linenos:
   :start-at: class TableSummation
   :end-at: };  // TableSummation
   :caption: Using visitor pattern that can compute sum of table with any numeric type
  

.. recipe:: ../code/basic_arrow.cc VisitorSummationExample
   :dedent: 2
