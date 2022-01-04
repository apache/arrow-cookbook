package usecase;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static java.util.Arrays.asList;

public class ProcessingData {
    // Scenario: Read twitter post for processing

    // Question: What is the average age per city that are talking about cryptocurrency for people between 21-27 years on twitter post
    /*
    name	    topic	        city	        age
    david	    cryptocurrency	lima	        21
    gladis	    fashion	        cuzco	        22
    juan	    cryptocurrency  huancayo	    35
    pedro	    healthcare	    tarapoto	    23
    oscar	    security	    lima	        27
    ronald	    cryptocurrency  lima	        44
    francisco   cryptocurrency	lima	        25
     */

    public static void main(String[] args) {
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE); // deal with byte buffer allocation
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(createSchema(), rootAllocator);
        populateData(vectorSchemaRoot);
        // render data
        String initialContentForProcessing = vectorSchemaRoot.contentToTSVString();
        System.out.println(initialContentForProcessing);
        // get index filter by age
        List ageFilter = getIndexFilterPerAge(vectorSchemaRoot);
        // get index filter by city
        List topicFilter = getIndexFilterPerTopic(vectorSchemaRoot);
        // cross index filter
        List globalIndexFilter = intersectionIndexFilter(ageFilter, topicFilter);
        // aggregations
        Map mapCountCityPerCrossFilter = new HashMap<String, Integer>();
        Map mapSumAgePerCrossFilter = new HashMap<Integer, Integer>();
        doAggregation(globalIndexFilter, mapCountCityPerCrossFilter, mapSumAgePerCrossFilter, vectorSchemaRoot);

        // report
        report(mapCountCityPerCrossFilter, mapSumAgePerCrossFilter);
    }

    private static void report(Map mapCountCityPerCrossFilter, Map mapSumAgePerCrossFilter){
        System.out.println(">>>>> REPORT <<<<< ");
        for ( Object keyCity : mapCountCityPerCrossFilter.keySet()) {
            int sumAgePerCrossFilter = (int) mapSumAgePerCrossFilter.get(keyCity);
            int countCityPerCrossFilter = (int) mapCountCityPerCrossFilter.get(keyCity);
            double ageAveragePerCity = sumAgePerCrossFilter / countCityPerCrossFilter;
            System.out.println("City: " + keyCity + ", Number of person: " + countCityPerCrossFilter + ", Age average talking about criptocurrency: " + ageAveragePerCity);
        }
    }

    private static void doAggregation(List<Integer> crossFilterIndex, Map mapCountCityPerCrossFilter, Map mapSumAgePerCrossFilter, VectorSchemaRoot vectorSchemaRoot){
        IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
        VarCharVector cityVector = (VarCharVector) vectorSchemaRoot.getVector("city");
        for(int index: crossFilterIndex){
            // city aggregation
            String currentCity = new String(cityVector.get(index), StandardCharsets.UTF_8);
            mapCountCityPerCrossFilter.put(currentCity, (Integer) mapCountCityPerCrossFilter.getOrDefault(currentCity, 0) + 1);
            // sum age aggregation per city
            mapSumAgePerCrossFilter.put(currentCity, (Integer) mapSumAgePerCrossFilter.getOrDefault(currentCity, 0) + ageVector.get(index));
        }
        System.out.println("City-count-aggregation: " + mapCountCityPerCrossFilter);
        System.out.println("City-sum-age-aggregation: "+mapSumAgePerCrossFilter);
    }


    private static List intersectionIndexFilter(List<Integer> firstIndex, List<Integer> secondIndex) {
        List ageAndCityIndexFilterIntersection = new ArrayList<Integer>();
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
        System.out.println("Cross-filter-index: " + ageAndCityIndexFilterIntersection);
        return ageAndCityIndexFilterIntersection;
    }


    private static List getIndexFilterPerAge(VectorSchemaRoot schemaRoot) {
        IntVector ageVector = (IntVector) schemaRoot.getVector("age");
        List ageSelectedIndexFilterPerAge = new ArrayList<Integer>();
        for (int i = 0; i < schemaRoot.getRowCount(); i++) {
            int current = ageVector.get(i);
            if (21 <= current && current <= 27) { // Get index for age between 21-27
                ageSelectedIndexFilterPerAge.add(i);
            }
        }
        System.out.println("Age filter index: " + ageSelectedIndexFilterPerAge);
        return ageSelectedIndexFilterPerAge;
    }


    private static List getIndexFilterPerTopic(VectorSchemaRoot schemaRoot) {
        VarCharVector topicVector = (VarCharVector) schemaRoot.getVector("topic");
        List ageSelectedIndexFilterPerCity = new ArrayList<Integer>();
        byte[] byteToSearch = "cryptocurrency".getBytes();
        for (int i = 0; i < schemaRoot.getRowCount(); i++) {
            if(Arrays.equals(topicVector.get(i), byteToSearch)){ // Get index for city equals to lima
                ageSelectedIndexFilterPerCity.add(i);
            }
        }
        System.out.println("City filter index: " + ageSelectedIndexFilterPerCity);
        return ageSelectedIndexFilterPerCity;
    }

    private static void setVector(IntVector vector, Integer... values) {
        final int length = values.length;
        vector.allocateNew(length);
        for (int i = 0; i < length; i++) {
            if (values[i] != null) {
                vector.set(i, values[i]);
            }
        }
        vector.setValueCount(length);
    }

    private static void setVector(VarCharVector vector, byte[]... values) {
        final int length = values.length;
        vector.allocateNewSafe();
        for (int i = 0; i < length; i++) {
            if (values[i] != null) {
                vector.set(i, values[i]);
            }
        }
        vector.setValueCount(length);
    }

    private static void populateData(VectorSchemaRoot vectorSchemaRoot){
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

        private static List<Field> createFields(){
            // create a column data type
            Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
            Field topic = new Field("topic", FieldType.nullable(new ArrowType.Utf8()), null);
            Field city = new Field("city", FieldType.nullable(new ArrowType.Utf8()), null);
            Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
            return asList(name, topic, city, age);
        }

        private static Schema createSchema(){
            return new Schema(createFields());
        }
}
