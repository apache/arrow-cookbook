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

Compare Values on the Array
===========================

Comparing two values at the given indices in the vectors:

.. testcode::

    import org.apache.arrow.algorithm.sort.StableVectorComparator;
    import org.apache.arrow.algorithm.sort.VectorValueComparator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.memory.RootAllocator;

    class TestVectorValueComparator extends VectorValueComparator<VarCharVector> {
        @Override
        public int compareNotNull(int index1, int index2) {
            byte b1 = vector1.get(index1)[0];
            byte b2 = vector2.get(index2)[0];
            return b1 - b2;
        }

        @Override
        public VectorValueComparator<VarCharVector> createNew() {
            return new TestVectorValueComparator();
        }
    }

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

    VarCharVector vec = new VarCharVector("valueindexcomparator", rootAllocator);
    vec.allocateNew(3);
    vec.setValueCount(3);
    vec.set(0, "ba".getBytes());
    vec.set(1, "abc".getBytes());
    vec.set(2, "aa".getBytes());

    VectorValueComparator<VarCharVector> comparatorValues = new TestVectorValueComparator();
    VectorValueComparator<VarCharVector> stableComparator = new StableVectorComparator<>(comparatorValues);
    stableComparator.attachVector(vec);

    System.out.println(stableComparator.compare(0, 1) > 0);
    System.out.println(stableComparator.compare(1, 2) < 0);

.. testoutput::

    true
    true

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
    List<Integer> listResultLinearSearch = new ArrayList<Integer>();
    for (int i = 0; i < 10; i++) {
       int result = VectorSearcher.linearSearch(linearSearchVector, comparatorInt, linearSearchVector, i);
       listResultLinearSearch.add(result);
    }

    System.out.println(listResultLinearSearch);

.. testoutput::

    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

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
    List<Integer> listResultBinarySearch = new ArrayList<Integer>();
    for (int i = 0; i < 10; i++) {
       int result = VectorSearcher.binarySearch(binarySearchVector, comparatorInt, binarySearchVector, i);
       listResultBinarySearch.add(result);
    }

    System.out.println(listResultBinarySearch);

.. testoutput::

    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

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

Data Filter & Aggregation
=====================================

Scenario
********

Scenario: Read data that contains twitter post for analytics

Question: What is the average age per city that are talking about cryptocurrency for people between 21-27 years on twitter

.. testcode::

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import java.nio.charset.StandardCharsets;

    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.HashMap;
    import java.util.List;
    import java.util.Map;

    import static java.util.Arrays.asList;

    // populate data
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field topic = new Field("topic", FieldType.nullable(new ArrowType.Utf8()), null);
    Field city = new Field("city", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    Schema schema = new Schema(asList(name, topic, city, age));
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator);
    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    nameVector.allocateNew(7);
    nameVector.set(0, "David".getBytes());
    nameVector.set(1, "Gladis".getBytes());
    nameVector.set(2, "Juan".getBytes());
    nameVector.set(3, "Pedro".getBytes());
    nameVector.set(4, "Oscar".getBytes());
    nameVector.set(5, "Ronald".getBytes());
    nameVector.set(6, "Francisco".getBytes());
    nameVector.setValueCount(7);
    VarCharVector topicVector = (VarCharVector) vectorSchemaRoot.getVector("topic");
    topicVector.allocateNew(7);
    topicVector.set(0, "Cryptocurrency".getBytes());
    topicVector.set(1, "Fashionmode".getBytes());
    topicVector.set(2, "Cryptocurrency".getBytes());
    topicVector.set(3, "Healthcare".getBytes());
    topicVector.set(4, "Security".getBytes());
    topicVector.set(5, "Cryptocurrency".getBytes());
    topicVector.set(6, "Cryptocurrency".getBytes());
    topicVector.setValueCount(7);
    VarCharVector cityVector = (VarCharVector) vectorSchemaRoot.getVector("city");
    cityVector.allocateNew(7);
    cityVector.set(0, "Lima".getBytes());
    cityVector.set(1, "Cuzco".getBytes());
    cityVector.set(2, "Huancayo".getBytes());
    cityVector.set(3, "Tarapoto".getBytes());
    cityVector.set(4, "Lima".getBytes());
    cityVector.set(5, "Lima".getBytes());
    cityVector.set(6, "Lima".getBytes());
    cityVector.setValueCount(7);
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(7);
    ageVector.set(0, 21);
    ageVector.set(1, 22);
    ageVector.set(2, 26);
    ageVector.set(3, 23);
    ageVector.set(4, 27);
    ageVector.set(5, 44);
    ageVector.set(6, 25);
    ageVector.setValueCount(7);
    vectorSchemaRoot.setRowCount(7);

    // Get Index Filter by Age Between 21-27
    List<Integer> ageSelectedIndexFilterPerAge = new ArrayList<Integer>();
    for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
        int current = ageVector.get(i);
        if (21 <= current && current <= 27) { // Get index for age between 21-27
            ageSelectedIndexFilterPerAge.add(i);
        }
    }

    // Get Index Filter by Topic Cryptocurrency
    List<Integer> ageSelectedIndexFilterPerTopic = new ArrayList<Integer>();
    byte[] byteToSearch = "Cryptocurrency".getBytes();
    for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
        if(Arrays.equals(topicVector.get(i), byteToSearch)){
            ageSelectedIndexFilterPerTopic.add(i);
        }
    }

    // Merge Index Filter: Age & Topic
    int indexAge = 0;
    int indexCity = 0;
    List<Integer> ageAndCityIndexFilterIntersection = new ArrayList<Integer>();
    while (indexAge < ageSelectedIndexFilterPerAge.size() && indexCity < ageSelectedIndexFilterPerTopic.size()) {
        if (ageSelectedIndexFilterPerAge.get(indexAge) < ageSelectedIndexFilterPerTopic.get(indexCity)) {
            indexAge++;
        } else if (ageSelectedIndexFilterPerAge.get(indexAge) > ageSelectedIndexFilterPerTopic.get(indexCity)) {
            indexCity++;
        } else {
            ageAndCityIndexFilterIntersection.add(ageSelectedIndexFilterPerAge.get(indexAge));
            indexAge++;
            indexCity++;
        }
    }

    // Aggregation
    Map<String, Integer> mapCountCityPerCrossFilter = new HashMap<String, Integer>();
    Map<String, Integer> mapSumAgePerCrossFilter = new HashMap<String, Integer>();
    for(int index: ageAndCityIndexFilterIntersection){
        // city aggregation
        String currentCity = new String(cityVector.get(index), StandardCharsets.UTF_8);
        mapCountCityPerCrossFilter.put(currentCity, (Integer) mapCountCityPerCrossFilter.getOrDefault(currentCity, 0) + 1);
        // sum age aggregation per city
        mapSumAgePerCrossFilter.put(currentCity, (Integer) mapSumAgePerCrossFilter.getOrDefault(currentCity, 0) + ageVector.get(index));
    }
    for ( Object keyCity : mapCountCityPerCrossFilter.keySet()) {
        int sumAgePerCrossFilter = (int) mapSumAgePerCrossFilter.get(keyCity);
        int countCityPerCrossFilter = (int) mapCountCityPerCrossFilter.get(keyCity);
        double ageAveragePerCity = sumAgePerCrossFilter / countCityPerCrossFilter;
        System.out.println("City: " + keyCity + ", Number of person: " + countCityPerCrossFilter + ", Age average talking about criptocurrency: " + ageAveragePerCity);
    }

.. testoutput::

    City: Lima, Number of person: 2, Age average talking about criptocurrency: 23.0
    City: Huancayo, Number of person: 1, Age average talking about criptocurrency: 26.0



