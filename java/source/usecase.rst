========
Use Case
========

Showcase to solve common use case tru apache java arrow


.. contents::


Use case -  Data Filter & Aggregation
===============================================

Scenario: Read data that contains twitter post for analytics

Question: What is the average age per city that are talking about cryptocurrency for people between 21-27 years on twitter post

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

Solving use case
****************

Get index filter by age between 21-27
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

Get index filter by topic cryptocurrency
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

Cross index filter: age & topic
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
   


