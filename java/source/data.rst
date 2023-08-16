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

=================
Data manipulation
=================

Recipes related to compare, filtering or transforming data.

.. contents::

Append VectorSchemaRoots
========================

In some cases, VectorSchemaRoot needs to be modeled as a container. To accomplish
this, you can use ``VectorSchemaRootAppender.append``. The following code reads a
Parquet file with three row groups, gets the three vectors separately, and then
appends the three vectors together:

.. testcode::

    import java.io.IOException;

    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.ipc.ArrowReader;
    import org.apache.arrow.vector.util.VectorSchemaRootAppender;

    VectorSchemaRoot appendVectorSchemaRootAsOne(BufferAllocator allocator) {
      VectorSchemaRoot result = null;
      final ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
      final String uri = "file:" + System.getProperty("user.dir") + "/thirdpartydeps/parquetfiles/data4_3rg_gzip.parquet";
      try (
          final DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator,
              NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
          final Dataset dataset = datasetFactory.finish();
          final Scanner scanner = dataset.newScan(options);
          final ArrowReader reader = scanner.scanBatches()
      ) {
        int rowgroup = 1;
        while (reader.loadNextBatch()) {
          try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
            if(result == null) {
               result = VectorSchemaRoot.create(root.getSchema(), allocator);
               result.allocateNew(); // allocate each vector before append data for memory purposes
            }
            System.out.println("Loading VectorSchemaRoot: " + rowgroup++ + ", Records to append: " + root.getRowCount());
            VectorSchemaRootAppender.append(result, root);
          }
        }
        return result;
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    // reading final VectorSchemaRoot
    try (final VectorSchemaRoot root = appendVectorSchemaRootAsOne(new RootAllocator())) {
      System.out.println("Total Rowcount: " + root.getRowCount());
      System.out.println(root.contentToTSVString());
    }

.. testoutput::

    Loading VectorSchemaRoot: 1, Records to append: 4
    Loading VectorSchemaRoot: 2, Records to append: 4
    Loading VectorSchemaRoot: 3, Records to append: 3
    Total Rowcount: 11
    age    name
    10    Jean
    10    Lu
    10    Kei
    10    Sophia
    10    Mara
    20    Arit
    20    Neil
    20    Jason
    20    John
    20    Peter
    20    Ismael

Compare Vectors for Field Equality
==================================

.. testcode::

   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.vector.compare.TypeEqualsVisitor;
   import org.apache.arrow.memory.RootAllocator;

   try(
       BufferAllocator allocator = new RootAllocator();
       IntVector right = new IntVector("int", allocator);
   ) {
       right.allocateNew(3);
       right.set(0, 10);
       right.set(1, 20);
       right.set(2, 30);
       right.setValueCount(3);
       IntVector left1 = new IntVector("int", allocator);
       IntVector left2 = new IntVector("int2", allocator);
       TypeEqualsVisitor visitor = new TypeEqualsVisitor(right);

       System.out.println(visitor.equals(left1));
       System.out.println(visitor.equals(left2));
   }

.. testoutput::

   true
   false

Compare Vectors Equality
========================

.. testcode::

   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.compare.VectorEqualsVisitor;

   try(
       BufferAllocator allocator = new RootAllocator();
       IntVector vector1 = new IntVector("vector1", allocator);
       IntVector vector2 = new IntVector("vector1", allocator);
       IntVector vector3 = new IntVector("vector1", allocator)
   ) {
       vector1.allocateNew(1);
       vector1.set(0, 10);
       vector1.setValueCount(1);

       vector2.allocateNew(1);
       vector2.set(0, 10);
       vector2.setValueCount(1);

       vector3.allocateNew(1);
       vector3.set(0, 20);
       vector3.setValueCount(1);
       VectorEqualsVisitor visitor = new VectorEqualsVisitor();

       System.out.println(visitor.vectorEquals(vector1, vector2));
       System.out.println(visitor.vectorEquals(vector1, vector3));
   }

.. testoutput::

   true
   false

Compare Values on the Array
===========================

Comparing two values at the given indices in the vectors:

.. testcode::

   import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
   import org.apache.arrow.algorithm.sort.VectorValueComparator;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.vector.VarCharVector;
   import org.apache.arrow.memory.RootAllocator;

   try(
       BufferAllocator allocator = new RootAllocator();
       VarCharVector vec = new VarCharVector("valueindexcomparator", allocator);
   ) {
       vec.allocateNew(3);
       vec.setValueCount(3);
       vec.set(0, "ba".getBytes());
       vec.set(1, "abc".getBytes());
       vec.set(2, "aa".getBytes());
       VectorValueComparator<VarCharVector> valueComparator = DefaultVectorComparators.createDefaultComparator(vec);
       valueComparator.attachVector(vec);

       System.out.println(valueComparator.compare(0, 1) > 0);
       System.out.println(valueComparator.compare(1, 2) < 0);
   }

.. testoutput::

   true
   false

Consider that if we need our own comparator we could extend VectorValueComparator
and override compareNotNull method as needed

Search Values on the Array
==========================

Linear Search - O(n)
********************

Algorithm: org.apache.arrow.algorithm.search.VectorSearcher#linearSearch - O(n)

.. testcode::

   import org.apache.arrow.algorithm.search.VectorSearcher;
   import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
   import org.apache.arrow.algorithm.sort.VectorValueComparator;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.memory.RootAllocator;

   try(
       BufferAllocator allocator = new RootAllocator();
       IntVector linearSearchVector = new IntVector("linearSearchVector", allocator);
   ) {
       linearSearchVector.allocateNew(10);
       linearSearchVector.setValueCount(10);
       for (int i = 0; i < 10; i++) {
           linearSearchVector.set(i, i);
       }
       VectorValueComparator<IntVector> comparatorInt = DefaultVectorComparators.createDefaultComparator(linearSearchVector);
       int result = VectorSearcher.linearSearch(linearSearchVector, comparatorInt, linearSearchVector, 3);

       System.out.println(result);
   }

.. testoutput::

   3

Binary Search - O(log(n))
*************************

Algorithm: org.apache.arrow.algorithm.search.VectorSearcher#binarySearch - O(log(n))

.. testcode::

   import org.apache.arrow.algorithm.search.VectorSearcher;
   import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
   import org.apache.arrow.algorithm.sort.VectorValueComparator;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.memory.RootAllocator;

   try(
       BufferAllocator allocator = new RootAllocator();
       IntVector binarySearchVector = new IntVector("", allocator);
   ) {
       binarySearchVector.allocateNew(10);
       binarySearchVector.setValueCount(10);
       for (int i = 0; i < 10; i++) {
           binarySearchVector.set(i, i);
       }
       VectorValueComparator<IntVector> comparatorInt = DefaultVectorComparators.createDefaultComparator(binarySearchVector);
       int result = VectorSearcher.binarySearch(binarySearchVector, comparatorInt, binarySearchVector, 3);

       System.out.println(result);
   }

.. testoutput::

   3

Sort Values on the Array
========================

In-place Sorter - O(nlog(n))
****************************

Sorting by manipulating the original vector.
Algorithm: org.apache.arrow.algorithm.sort.FixedWidthInPlaceVectorSorter - O(nlog(n))

.. testcode::

   import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
   import org.apache.arrow.algorithm.sort.FixedWidthInPlaceVectorSorter;
   import org.apache.arrow.algorithm.sort.VectorValueComparator;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.memory.RootAllocator;

   try(
       BufferAllocator allocator = new RootAllocator();
       IntVector intVectorNotSorted = new IntVector("intvectornotsorted", allocator);
   ) {
       intVectorNotSorted.allocateNew(3);
       intVectorNotSorted.setValueCount(3);
       intVectorNotSorted.set(0, 10);
       intVectorNotSorted.set(1, 8);
       intVectorNotSorted.setNull(2);
       FixedWidthInPlaceVectorSorter<IntVector> sorter = new FixedWidthInPlaceVectorSorter<IntVector>();
       VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(intVectorNotSorted);
       sorter.sortInPlace(intVectorNotSorted, comparator);

       System.out.println(intVectorNotSorted);
   }

.. testoutput::

   [null, 8, 10]

Out-place Sorter - O(nlog(n))
*****************************

Sorting by copies vector elements to a new vector in sorted order - O(nlog(n))
Algorithm: : org.apache.arrow.algorithm.sort.FixedWidthInPlaceVectorSorter.
FixedWidthOutOfPlaceVectorSorter & VariableWidthOutOfPlaceVectorSor

.. testcode::

   import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
   import org.apache.arrow.algorithm.sort.FixedWidthOutOfPlaceVectorSorter;
   import org.apache.arrow.algorithm.sort.OutOfPlaceVectorSorter;
   import org.apache.arrow.algorithm.sort.VectorValueComparator;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.memory.RootAllocator;

   try(
       BufferAllocator allocator = new RootAllocator();
       IntVector intVectorNotSorted = new IntVector("intvectornotsorted", allocator);
       IntVector intVectorSorted = (IntVector) intVectorNotSorted.getField()
               .getFieldType().createNewSingleVector("new-out-of-place-sorter",
                       allocator, null);

   ) {
       intVectorNotSorted.allocateNew(3);
       intVectorNotSorted.setValueCount(3);
       intVectorNotSorted.set(0, 10);
       intVectorNotSorted.set(1, 8);
       intVectorNotSorted.setNull(2);
       OutOfPlaceVectorSorter<IntVector> sorterOutOfPlaceSorter = new FixedWidthOutOfPlaceVectorSorter<>();
       VectorValueComparator<IntVector> comparatorOutOfPlaceSorter = DefaultVectorComparators.createDefaultComparator(intVectorNotSorted);
       intVectorSorted.allocateNew(intVectorNotSorted.getValueCount());
       intVectorSorted.setValueCount(intVectorNotSorted.getValueCount());
       sorterOutOfPlaceSorter.sortOutOfPlace(intVectorNotSorted, intVectorSorted, comparatorOutOfPlaceSorter);

       System.out.println(intVectorSorted);
   }

.. testoutput::

   [null, 8, 10]
