=================
Data manipulation
=================

Recipes related to compare, filtering or transforming data.

.. contents::

We are going to use this util for data manipulation:

.. code-block:: java

   import org.apache.arrow.algorithm.sort.VectorValueComparator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.vector.VarCharVector;

   void setVector(IntVector vector, Integer... values) {
      final int length = values.length;
      vector.allocateNew(length);
      for (int i = 0; i < length; i++) {
          if (values[i] != null) {
              vector.set(i, values[i]);
          }
      }
      vector.setValueCount(length);
   }

  class TestVarCharSorter extends VectorValueComparator<VarCharVector> {
    @Override
    public int compareNotNull(int index1, int index2) {
        byte b1 = vector1.get(index1)[0];
        byte b2 = vector2.get(index2)[0];
        return b1 - b2;
    }

    @Override
    public VectorValueComparator<VarCharVector> createNew() {
        return new TestVarCharSorter();
    }
  }
  RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE); // deal with byte buffer allocation

Compare fields on the array
===========================

.. code-block:: java
   :emphasize-lines: 10

   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.vector.compare.TypeEqualsVisitor;

   IntVector right = new IntVector("int", rootAllocator);
   IntVector left1 = new IntVector("int", rootAllocator);
   IntVector left2 = new IntVector("int2", rootAllocator);

   setVector(right, 10,20,30);

   TypeEqualsVisitor visitor = new TypeEqualsVisitor(right); // equal or unequal

Comparing vector fields:

.. code-block:: java
   :emphasize-lines: 1-4

   jshell> visitor.equals(left1); visitor.equals(left2);

   true
   false

Compare values on the array
===========================

.. code-block:: java
   :emphasize-lines: 15-17

   import org.apache.arrow.algorithm.sort.StableVectorComparator;
   import org.apache.arrow.algorithm.sort.VectorValueComparator;
   import org.apache.arrow.vector.VarCharVector;

   // compare two values at the given indices in the vectors.
   // comparing org.apache.arrow.algorithm.sort.VectorValueComparator on algorithm
   VarCharVector vec = new VarCharVector("valueindexcomparator", rootAllocator);
   vec.allocateNew(100, 5);
   vec.setValueCount(10);
   vec.set(0, "ba".getBytes());
   vec.set(1, "abc".getBytes());
   vec.set(2, "aa".getBytes());
   vec.set(3, "abc".getBytes());
   vec.set(4, "a".getBytes());
   VectorValueComparator<VarCharVector> comparatorValues = new TestVarCharSorter(); // less than, equal to, greater than
   VectorValueComparator<VarCharVector> stableComparator = new StableVectorComparator<>(comparatorValues);//Stable comparator only supports comparing values from the same vector
   stableComparator.attachVector(vec);

Comparing two values at the given indices in the vectors:

.. code-block:: java
   :emphasize-lines: 1-8

   jshell> stableComparator.compare(0, 1) > 0; stableComparator.compare(1, 2) < 0; stableComparator.compare(2, 3) < 0; stableComparator.compare(1, 3) < 0; stableComparator.compare(3, 1) > 0; stableComparator.compare(3, 3) == 0;

   true
   true
   true
   true
   true
   true

Search values on the array
==========================

Linear search - O(n)
********************

Algorithm: org.apache.arrow.algorithm.search.VectorSearcher#linearSearch - O(n)

.. code-block:: java
   :emphasize-lines: 27

   import org.apache.arrow.algorithm.search.VectorSearcher;
   import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
   import org.apache.arrow.algorithm.sort.VectorValueComparator;
   import org.apache.arrow.vector.IntVector;

   // search values on the array
   // linear search org.apache.arrow.algorithm.search.VectorSearcher#linearSearch - O(n)
   IntVector rawVector = new IntVector("", rootAllocator);
   IntVector negVector = new IntVector("", rootAllocator);
   rawVector.allocateNew(10);
   rawVector.setValueCount(10);
   negVector.allocateNew(1);
   negVector.setValueCount(1);
   for (int i = 0; i < 10; i++) { // prepare data in sorted order
    if (i == 0) {
        rawVector.setNull(i);
    } else {
        rawVector.set(i, i);
    }
   }
   negVector.set(0, -333);
   VectorValueComparator<IntVector> comparatorInt = DefaultVectorComparators.createDefaultComparator(rawVector);

   // do search
   List<Integer> listResultLinearSearch = new ArrayList<Integer>();
   for (int i = 0; i < 10; i++) {
      int result = VectorSearcher.linearSearch(rawVector, comparatorInt, rawVector, i);
      listResultLinearSearch.add(result);
   }

Verify results:

.. code-block:: java
   :emphasize-lines: 1-3
   
   jshell> listResultLinearSearch

   listResultLinearSearch ==> [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

Binary search - O(log(n))
*************************

Algorithm: org.apache.arrow.algorithm.search.VectorSearcher#binarySearch - O(log(n))

.. code-block:: java
   :emphasize-lines: 27

   import org.apache.arrow.algorithm.search.VectorSearcher;
   import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
   import org.apache.arrow.algorithm.sort.VectorValueComparator;
   import org.apache.arrow.vector.IntVector;

   // search values on the array
   // linear search org.apache.arrow.algorithm.search.VectorSearcher#linearSearch - O(n)
   IntVector rawVector = new IntVector("", rootAllocator);
   IntVector negVector = new IntVector("", rootAllocator);
   rawVector.allocateNew(10);
   rawVector.setValueCount(10);
   negVector.allocateNew(1);
   negVector.setValueCount(1);
   for (int i = 0; i < 10; i++) { // prepare data in sorted order
    if (i == 0) {
        rawVector.setNull(i);
    } else {
        rawVector.set(i, i);
    }
   }
   negVector.set(0, -333);
   VectorValueComparator<IntVector> comparatorInt = DefaultVectorComparators.createDefaultComparator(rawVector);

   // do search
   List<Integer> listResultBinarySearch = new ArrayList<Integer>();
   for (int i = 0; i < 10; i++) {
      int result = VectorSearcher.binarySearch(rawVector, comparatorInt, rawVector, i);
      listResultBinarySearch.add(result);
   }

Verify results:

.. code-block:: java
   :emphasize-lines: 1-3

   jshell> listResultBinarySearch

   listResultBinarySearch ==> [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

Sort values on the array
========================

In-place sorter - O(nlog(n))
****************************

Sorting by manipulating the original vector.
Algorithm: org.apache.arrow.algorithm.sort.FixedWidthInPlaceVectorSorter - O(nlog(n))

.. code-block:: java
   :emphasize-lines: 22-24

   import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
   import org.apache.arrow.algorithm.sort.FixedWidthInPlaceVectorSorter;
   import org.apache.arrow.algorithm.sort.VectorValueComparator;
   import org.apache.arrow.vector.IntVector;

   // Sort the vector - In-place sorter
   IntVector vecToSort = new IntVector("in-place-sorter", rootAllocator);
   vecToSort.allocateNew(10);
   vecToSort.setValueCount(10);
   // fill data to sort
   vecToSort.set(0, 10);
   vecToSort.set(1, 8);
   vecToSort.setNull(2);
   vecToSort.set(3, 10);
   vecToSort.set(4, 12);
   vecToSort.set(5, 17);
   vecToSort.setNull(6);
   vecToSort.set(7, 23);
   vecToSort.set(8, 35);
   vecToSort.set(9, 2);
   // sort the vector
   FixedWidthInPlaceVectorSorter sorter = new FixedWidthInPlaceVectorSorter();
   VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(vecToSort);
   sorter.sortInPlace(vecToSort, comparator);

Verify results:

.. code-block:: java
   :emphasize-lines: 1-13

   jshell> vecToSort.getValueCount()==10; vecToSort.isNull(0); vecToSort.isNull(1); 2==vecToSort.get(2); 8==vecToSort.get(3); 10==vecToSort.get(4); 10==vecToSort.get(5); 12==vecToSort.get(6); 17==vecToSort.get(7); 23==vecToSort.get(8); 35==vecToSort.get(9);

   true
   true
   true
   true
   true
   true
   true
   true
   true
   true
   true

Out-place sorter - O(nlog(n))
*****************************

Sorting by copies vector elements to a new vector in sorted order - O(nlog(n))
Algorithm: : org.apache.arrow.algorithm.sort.FixedWidthInPlaceVectorSorter.
FixedWidthOutOfPlaceVectorSorter & VariableWidthOutOfPlaceVectorSor

.. code-block:: java
   :emphasize-lines: 20-25

   import org.apache.arrow.algorithm.sort.*;
   import org.apache.arrow.vector.IntVector;

   // Sort the vector - Out-of-place sorter:
   IntVector vecOutOfPlaceSorter = new IntVector("out-of-place-sorter", rootAllocator);
   vecOutOfPlaceSorter.allocateNew(10);
   vecOutOfPlaceSorter.setValueCount(10);
   // fill data to sort
   vecOutOfPlaceSorter.set(0, 10);
   vecOutOfPlaceSorter.set(1, 8);
   vecOutOfPlaceSorter.setNull(2);
   vecOutOfPlaceSorter.set(3, 10);
   vecOutOfPlaceSorter.set(4, 12);
   vecOutOfPlaceSorter.set(5, 17);
   vecOutOfPlaceSorter.setNull(6);
   vecOutOfPlaceSorter.set(7, 23);
   vecOutOfPlaceSorter.set(8, 35);
   vecOutOfPlaceSorter.set(9, 2);
   // sort the vector
   OutOfPlaceVectorSorter<IntVector> sorterOutOfPlaceSorter = new FixedWidthOutOfPlaceVectorSorter<>();
   VectorValueComparator<IntVector> comparatorOutOfPlaceSorter = DefaultVectorComparators.createDefaultComparator(vecOutOfPlaceSorter);
   IntVector sortedVec = (IntVector) vecOutOfPlaceSorter.getField().getFieldType().createNewSingleVector("new-out-of-place-sorter", rootAllocator, null);
   sortedVec.allocateNew(vecOutOfPlaceSorter.getValueCount());
   sortedVec.setValueCount(vecOutOfPlaceSorter.getValueCount());
   sorterOutOfPlaceSorter.sortOutOfPlace(vecOutOfPlaceSorter, sortedVec, comparatorOutOfPlaceSorter);

Verify results:

.. code-block:: java
   :emphasize-lines: 1-13

   jshell> ecOutOfPlaceSorter.getValueCount()==sortedVec.getValueCount(); sortedVec.isNull(0 );sortedVec.isNull(1); 2==sortedVec.get(2); 8==sortedVec.get(3); 10==sortedVec.get(4); 10==sortedVec.get(5); 12==sortedVec.get(6); 17==sortedVec.get(7); 23==sortedVec.get(8); 35==sortedVec.get(9);

   true
   true
   true
   true
   true
   true
   true
   true
   true
   true
   true

