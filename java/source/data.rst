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
   RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE); // deal with byte buffer allocation

Compare Vectors for Field Equality
==================================

.. code-block:: java
   :emphasize-lines: 10

   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.vector.compare.TypeEqualsVisitor;

   IntVector right = new IntVector("int", rootAllocator);
   IntVector left1 = new IntVector("int", rootAllocator);
   IntVector left2 = new IntVector("int2", rootAllocator);

   setVector(right, 10,20,30);

   TypeEqualsVisitor visitor = new TypeEqualsVisitor(right);

Comparing vector fields:

.. code-block:: java
   :emphasize-lines: 1-4

   jshell> visitor.equals(left1);
   true
   jshell> visitor.equals(left2);
   false

Compare Values on the Array
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
   VectorValueComparator<VarCharVector> comparatorValues = new TestVectorValueComparator(); // less than, equal to, greater than
   VectorValueComparator<VarCharVector> stableComparator = new StableVectorComparator<>(comparatorValues);//Stable comparator only supports comparing values from the same vector
   stableComparator.attachVector(vec);

Comparing two values at the given indices in the vectors:

.. code-block:: java
   :emphasize-lines: 1-12

   jshell> stableComparator.compare(0, 1) > 0;
   true 
   jshell> stableComparator.compare(1, 2) < 0;
   true 
   jshell> stableComparator.compare(2, 3) < 0;
   true 
   jshell> stableComparator.compare(1, 3) < 0;
   true 
   jshell> stableComparator.compare(3, 1) > 0;
   true 
   jshell> stableComparator.compare(3, 3) == 0;
   true

Search Values on the Array
==========================

Linear Search - O(n)
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

Binary Search - O(log(n))
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

Sort Values on the Array
========================

In-place Sorter - O(nlog(n))
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
   :emphasize-lines: 1-22

   jshell> vecToSort.getValueCount()==10;
   true 
   jshell> vecToSort.isNull(0);
   true 
   jshell> vecToSort.isNull(1);
   true 
   jshell> 2==vecToSort.get(2);
   true 
   jshell> 8==vecToSort.get(3);
   true 
   jshell> 10==vecToSort.get(4);
   true 
   jshell> 10==vecToSort.get(5);
   true 
   jshell> 12==vecToSort.get(6);
   true 
   jshell> 17==vecToSort.get(7);
   true 
   jshell> 23==vecToSort.get(8);
   true 
   jshell> 35==vecToSort.get(9);
   true

Out-place Sorter - O(nlog(n))
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
   :emphasize-lines: 1-22

   jshell> vecOutOfPlaceSorter.getValueCount()==sortedVec.getValueCount();
   true 
   jshell> sortedVec.isNull(0 );
   true
   jshell> sortedVec.isNull(1); 
   true
   jshell> 2==sortedVec.get(2); 
   true
   jshell> 8==sortedVec.get(3);
   true 
   jshell> 10==sortedVec.get(4); 
   true
   jshell> 10==sortedVec.get(5);
   true 
   jshell> 12==sortedVec.get(6); 
   true
   jshell> 17==sortedVec.get(7); 
   true
   jshell> 23==sortedVec.get(8); 
   true
   jshell> 35==sortedVec.get(9);
   true

Use Case -  Data Filter & Aggregation
=====================================

Scenario
********

Scenario: Read data that contains twitter post for analytics

Question: What is the average age per city that are talking about cryptocurrency for people between 21-27 years on twitter post

Solving Use Case
****************

Util Functions
--------------

We are going to use this util for our use case -  data filter & aggregation

.. code-block:: java

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.Schema;

    import java.util.List;

    import static java.util.Arrays.asList;

    // define fields
    List<Field> createFields(){
        // create a column data type
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field topic = new Field("topic", FieldType.nullable(new ArrowType.Utf8()), null);
        Field city = new Field("city", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        return asList(name, topic, city, age);
    }

    // create schema
    private static Schema createSchema(){
        return new Schema(createFields());
    }

    // create the vector schema root
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE); // deal with byte buffer allocation
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(createSchema(), rootAllocator);

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

    void setVector(VarCharVector vector, byte[]... values) {
        final int length = values.length;
        vector.allocateNewSafe();
        for (int i = 0; i < length; i++) {
            if (values[i] != null) {
                vector.set(i, values[i]);
            }
        }
        vector.setValueCount(length);
    }

    // populate data
    void populateData(VectorSchemaRoot vectorSchemaRoot){
        VarCharVector name = (VarCharVector) vectorSchemaRoot.getVector("name"); //interface FieldVector
        VarCharVector city = (VarCharVector) vectorSchemaRoot.getVector("city"); //interface FieldVector
        VarCharVector topic = (VarCharVector) vectorSchemaRoot.getVector("topic"); //interface FieldVector
        IntVector age = (IntVector) vectorSchemaRoot.getVector("age");
        // add values to the field vectors
        setVector(name, "david".getBytes(), "gladis".getBytes(), "juan".getBytes(), "pedro".getBytes(), "oscar".getBytes(), "ronald".getBytes(), "francisco".getBytes());
        setVector(city, "lima".getBytes(), "cuzco".getBytes(), "huancayo".getBytes(), "tarapoto".getBytes(), "lima".getBytes(), "lima".getBytes(), "lima".getBytes());
        setVector(topic, "cryptocurrency".getBytes(), "fashion".getBytes(), "cryptocurrency".getBytes(), "healthcare".getBytes(), "security".getBytes(), "cryptocurrency".getBytes(), "cryptocurrency".getBytes());
        setVector(age, 21, 22, 26, 23, 27, 44, 25);
        vectorSchemaRoot.setRowCount(7);
    }

    populateData(vectorSchemaRoot);


Render data:

.. code-block:: java

   jshell> System.out.println(vectorSchemaRoot.contentToTSVString());

   name        topic          city     age
   david       cryptocurrency lima     21
   gladis      fashion        cuzco    22
   juan        cryptocurrency huancayo 26
   pedro       healthcare     tarapoto 23
   oscar       security       lima     27
   ronald      cryptocurrency lima     44
   francisco   cryptocurrency lima     25

Get Index Filter by Age Between 21-27
-------------------------------------

.. code-block:: java

    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.VectorSchemaRoot;

    import java.util.ArrayList;
    import java.util.List;

    List ageSelectedIndexFilterPerAge = new ArrayList<Integer>();

    void getIndexFilterPerAge(VectorSchemaRoot schemaRoot) {
        IntVector ageVector = (IntVector) schemaRoot.getVector("age");

        for (int i = 0; i < schemaRoot.getRowCount(); i++) {
            int current = ageVector.get(i);
            if (21 <= current && current <= 27) { // Get index for age between 21-27
                ageSelectedIndexFilterPerAge.add(i);
            }
        }
    }

.. code-block:: java

   jshell> getIndexFilterPerAge(vectorSchemaRoot)

   jshell> ageSelectedIndexFilterPerAge
   ageSelectedIndexFilterPerAge ==> [0, 1, 2, 3, 4, 6]

Get Index Filter by Topic Cryptocurrency
----------------------------------------

.. code-block:: java

    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.VectorSchemaRoot;

    import java.util.ArrayList;
    import java.util.List;

    List ageSelectedIndexFilterPerTopic = new ArrayList<Integer>();

    void getIndexFilterPerTopic(VectorSchemaRoot schemaRoot) {
        VarCharVector topicVector = (VarCharVector) schemaRoot.getVector("topic");
        byte[] byteToSearch = "cryptocurrency".getBytes();

        for (int i = 0; i < schemaRoot.getRowCount(); i++) {
            if(Arrays.equals(topicVector.get(i), byteToSearch)){ // Get index for city equals to lima
                ageSelectedIndexFilterPerTopic.add(i);
            }
        }
    }

.. code-block:: java

   jshell> getIndexFilterPerTopic(vectorSchemaRoot)

   jshell> ageSelectedIndexFilterPerTopic
   ageSelectedIndexFilterPerTopic ==> [0, 2, 5, 6]

Cross Index Filter: Age & Topic
-------------------------------

.. code-block:: java

    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.VectorSchemaRoot;

    import java.util.ArrayList;
    import java.util.List;

    List ageAndCityIndexFilterIntersection = new ArrayList<Integer>();

    void intersectionIndexFilter(List<Integer> firstIndex, List<Integer> secondIndex) {

        int indexAge = 0;
        int indexCity = 0;

        while (indexAge < firstIndex.size() && indexCity < secondIndex.size()) {
            if (firstIndex.get(indexAge) < secondIndex.get(indexCity)) {
                indexAge++;
            } else if (firstIndex.get(indexAge) > secondIndex.get(indexCity)) {
                indexCity++;
            } else {
                ageAndCityIndexFilterIntersection.add(firstIndex.get(indexAge));
                indexAge++;
                indexCity++;
            }
        }
    }

.. code-block:: java

   jshell> intersectionIndexFilter(ageSelectedIndexFilterPerAge, ageSelectedIndexFilterPerTopic)

   jshell> ageAndCityIndexFilterIntersection
   ageAndCityIndexFilterIntersection ==> [0, 2, 6]


Aggregation
-----------

.. code-block:: java

    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.VectorSchemaRoot;

    import java.nio.charset.StandardCharsets;
    import java.util.List;
    import java.util.Map;

    Map mapCountCityPerCrossFilter = new HashMap<String, Integer>();
    Map mapSumAgePerCrossFilter = new HashMap<Integer, Integer>();

    void doAggregation(List<Integer> crossFilterIndex, Map mapCountCityPerCrossFilter, Map mapSumAgePerCrossFilter, VectorSchemaRoot vectorSchemaRoot){
        IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
        VarCharVector cityVector = (VarCharVector) vectorSchemaRoot.getVector("city");
        for(int index: crossFilterIndex){
            // city aggregation
            String currentCity = new String(cityVector.get(index), StandardCharsets.UTF_8);
            mapCountCityPerCrossFilter.put(currentCity, (Integer) mapCountCityPerCrossFilter.getOrDefault(currentCity, 0) + 1);
            // sum age aggregation per city
            mapSumAgePerCrossFilter.put(currentCity, (Integer) mapSumAgePerCrossFilter.getOrDefault(currentCity, 0) + ageVector.get(index));
        }
    }

.. code-block:: java

   jshell> doAggregation(ageAndCityIndexFilterIntersection, mapCountCityPerCrossFilter, mapSumAgePerCrossFilter, vectorSchemaRoot);

   jshell> mapCountCityPerCrossFilter
   mapCountCityPerCrossFilter ==> {lima=2, huancayo=1}

   jshell> mapSumAgePerCrossFilter
   mapSumAgePerCrossFilter ==> {lima=46, huancayo=26}


Report
******

.. code-block:: java

    import java.util.Map;

    void report(Map mapCountCityPerCrossFilter, Map mapSumAgePerCrossFilter){
        System.out.println(">>>>> REPORT <<<<< ");
        for ( Object keyCity : mapCountCityPerCrossFilter.keySet()) {
            int sumAgePerCrossFilter = (int) mapSumAgePerCrossFilter.get(keyCity);
            int countCityPerCrossFilter = (int) mapCountCityPerCrossFilter.get(keyCity);
            double ageAveragePerCity = sumAgePerCrossFilter / countCityPerCrossFilter;
            System.out.println("City: " + keyCity + ", Number of person: " + countCityPerCrossFilter + ", Age average talking about criptocurrency: " + ageAveragePerCity);
        }
    }

.. code-block:: java

   jshell> report(mapCountCityPerCrossFilter, mapSumAgePerCrossFilter);

   >>>>> REPORT <<<<<
   City: lima, Number of person: 2, Age average talking about criptocurrency: 23.0
   City: huancayo, Number of person: 1, Age average talking about criptocurrency: 26.0