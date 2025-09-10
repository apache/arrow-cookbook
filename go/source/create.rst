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

Recipes related to the creation of Arrays
.. contents::

Creating Arrays
===============

Arrow keeps data in continuous arrays optimised for memory footprint
and SIMD analyses.

.. testcode::

    import (
	    "fmt"

	    "github.com/apache/arrow/go/arrow"
	    "github.com/apache/arrow/go/arrow/array"
	    "github.com/apache/arrow/go/arrow/memory"
    )

    func main() {
	    pool := memory.NewGoAllocator()

	    lb := array.NewFixedSizeListBuilder(pool, 3, arrow.PrimitiveTypes.Int64)
	    defer lb.Release()

	    vb := lb.ValueBuilder().(*array.Int64Builder)
	    vb.Reserve(10)

	    lb.Append(true)
	    vb.Append(0)
	    vb.Append(1)
	    vb.Append(2)

	    lb.AppendNull()
	    vb.AppendValues([]int64{-1, -1, -1}, nil)

	    lb.Append(true)
	    vb.Append(3)
	    vb.Append(4)
	    vb.Append(5)

	    lb.Append(true)
	    vb.Append(6)
	    vb.Append(7)
	    vb.Append(8)

	    lb.AppendNull()

	    arr := lb.NewArray().(*array.FixedSizeList)
	    defer arr.Release()

	    fmt.Printf("NullN()   = %d\n", arr.NullN())
	    fmt.Printf("Len()     = %d\n", arr.Len())
	    fmt.Printf("Type()    = %v\n", arr.DataType())
	    fmt.Printf("List      = %v\n", arr)
    }

.. testcode::

.. testoutput::
    NullN()   = 2
    Len()     = 5
    Type()    = fixed_size_list<item: int64, nullable>[3]
    List      = [[0 1 2] (null) [3 4 5] [6 7 8] (null)]