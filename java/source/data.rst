=================
Data manipulation
=================

Recipes related to compare, filtering or transforming data.

.. contents::

Compare Vectors for Field Equality
==================================

.. testcode::

    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.compare.TypeEqualsVisitor;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    IntVector right = new IntVector("int", rootAllocator);
    right.allocateNew(3);
    right.set(0, 10);
    right.set(1, 20);
    right.set(2, 30);
    right.setValueCount(3);
    IntVector left1 = new IntVector("int", rootAllocator);
    IntVector left2 = new IntVector("int2", rootAllocator);
    TypeEqualsVisitor visitor = new TypeEqualsVisitor(right);

    System.out.println(visitor.equals(left1));
    System.out.println(visitor.equals(left2));

.. testoutput::

    true
    false

Compare Vectors Equality
========================

.. testcode::

    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.compare.VectorEqualsVisitor;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    IntVector vector1 = new IntVector("vector1", rootAllocator);
    vector1.allocateNew(1);
    vector1.set(0, 10);
    vector1.setValueCount(1);
    IntVector vector2 = new IntVector("vector1", rootAllocator);
    vector2.allocateNew(1);
    vector2.set(0, 10);
    vector2.setValueCount(1);
    IntVector vector3 = new IntVector("vector1", rootAllocator);
    vector3.allocateNew(1);
    vector3.set(0, 20);
    vector3.setValueCount(1);
    VectorEqualsVisitor visitor = new VectorEqualsVisitor();

    System.out.println(visitor.vectorEquals(vector1, vector2));
    System.out.println(visitor.vectorEquals(vector1, vector3));

.. testoutput::

    true
    false

Compare Values on the Array
===========================

Comparing two values at the given indices in the vectors:

.. testcode::

    import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
    import org.apache.arrow.algorithm.sort.VectorValueComparator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    VarCharVector vec = new VarCharVector("valueindexcomparator", rootAllocator);
    vec.allocateNew(3);
    vec.setValueCount(3);
    vec.set(0, "ba".getBytes());
    vec.set(1, "abc".getBytes());
    vec.set(2, "aa".getBytes());
    VectorValueComparator<VarCharVector> valueComparator = DefaultVectorComparators.createDefaultComparator(vec);
    valueComparator.attachVector(vec);

    System.out.println(valueComparator.compare(0, 1) > 0);
    System.out.println(valueComparator.compare(1, 2) < 0);

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
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    IntVector linearSearchVector = new IntVector("linearSearchVector", rootAllocator);
    linearSearchVector.allocateNew(10);
    linearSearchVector.setValueCount(10);
    for (int i = 0; i < 10; i++) {
        linearSearchVector.set(i, i);
    }
    VectorValueComparator<IntVector> comparatorInt = DefaultVectorComparators.createDefaultComparator(linearSearchVector);
    int result = VectorSearcher.linearSearch(linearSearchVector, comparatorInt, linearSearchVector, 3);

    System.out.println(result);

.. testoutput::

    3

Binary Search - O(log(n))
*************************

Algorithm: org.apache.arrow.algorithm.search.VectorSearcher#binarySearch - O(log(n))

.. testcode::

    import org.apache.arrow.algorithm.search.VectorSearcher;
    import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
    import org.apache.arrow.algorithm.sort.VectorValueComparator;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    IntVector binarySearchVector = new IntVector("", rootAllocator);
    binarySearchVector.allocateNew(10);
    binarySearchVector.setValueCount(10);
    for (int i = 0; i < 10; i++) {
        binarySearchVector.set(i, i);
    }
    VectorValueComparator<IntVector> comparatorInt = DefaultVectorComparators.createDefaultComparator(binarySearchVector);
    int result = VectorSearcher.binarySearch(binarySearchVector, comparatorInt, binarySearchVector, 3);

    System.out.println(result);

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
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    IntVector intVectorNotSorted = new IntVector("intvectornotsorted", rootAllocator);
    intVectorNotSorted.allocateNew(3);
    intVectorNotSorted.setValueCount(3);
    intVectorNotSorted.set(0, 10);
    intVectorNotSorted.set(1, 8);
    intVectorNotSorted.setNull(2);
    FixedWidthInPlaceVectorSorter<IntVector> sorter = new FixedWidthInPlaceVectorSorter<IntVector>();
    VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(intVectorNotSorted);
    sorter.sortInPlace(intVectorNotSorted, comparator);

    System.out.println(intVectorNotSorted);

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
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    IntVector intVectorNotSorted = new IntVector("intvectornotsorted", rootAllocator);
    intVectorNotSorted.allocateNew(3);
    intVectorNotSorted.setValueCount(3);
    intVectorNotSorted.set(0, 10);
    intVectorNotSorted.set(1, 8);
    intVectorNotSorted.setNull(2);
    OutOfPlaceVectorSorter<IntVector> sorterOutOfPlaceSorter = new FixedWidthOutOfPlaceVectorSorter<>();
    VectorValueComparator<IntVector> comparatorOutOfPlaceSorter = DefaultVectorComparators.createDefaultComparator(intVectorNotSorted);
    IntVector intVectorSorted = (IntVector) intVectorNotSorted.getField().getFieldType().createNewSingleVector("new-out-of-place-sorter", rootAllocator, null);
    intVectorSorted.allocateNew(intVectorNotSorted.getValueCount());
    intVectorSorted.setValueCount(intVectorNotSorted.getValueCount());
    sorterOutOfPlaceSorter.sortOutOfPlace(intVectorNotSorted, intVectorSorted, comparatorOutOfPlaceSorter);

    System.out.println(intVectorSorted);

.. testoutput::

    [null, 8, 10]
