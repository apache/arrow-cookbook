package data;

import org.apache.arrow.algorithm.search.VectorSearcher;
import org.apache.arrow.algorithm.sort.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.compare.TypeEqualsVisitor;

public class Cookbook {
    public static void main(String[] args) {
        // comparing vector fields org.apache.arrow.vector.compare.VectorValueEqualizer on vector module
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE); // deal with byte buffer allocation
        IntVector right = new IntVector("int", rootAllocator);
        IntVector left1 = new IntVector("int", rootAllocator);
        IntVector left2 = new IntVector("int2", rootAllocator);

        Util.setVector(right, 10,20,30);

        TypeEqualsVisitor visitor = new TypeEqualsVisitor(right); // equal or unequal
        System.out.println("Comparing vector fields:");
        System.out.println(visitor.equals(left1)); // compareField(left.getField(), right.getField())
        System.out.println(visitor.equals(left2));

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
        VectorValueComparator<VarCharVector> comparatorValues = new Util.TestVarCharSorter(); // less than, equal to, greater than
        VectorValueComparator<VarCharVector> stableComparator = new StableVectorComparator<>(comparatorValues);//Stable comparator only supports comparing values from the same vector
        stableComparator.attachVector(vec);
        System.out.println("Comparing two values at the given indices in the vectors:");
        System.out.println(stableComparator.compare(0, 1) > 0);
        System.out.println(stableComparator.compare(1, 2) < 0);
        System.out.println(stableComparator.compare(2, 3) < 0);
        System.out.println(stableComparator.compare(1, 3) < 0);
        System.out.println(stableComparator.compare(3, 1) > 0);
        System.out.println(stableComparator.compare(3, 3) == 0);

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
        VectorValueComparator<IntVector> comparatorInt = DefaultVectorComparators.createDefaultComparator(rawVector); // do search
        System.out.println("Linear search:");
        for (int i = 0; i < 10; i++) {
            int result = VectorSearcher.linearSearch(rawVector, comparatorInt, rawVector, i);
            System.out.println(result);
        }
        // negative case
        System.out.println(VectorSearcher.linearSearch(rawVector, comparatorInt, negVector, 0));

        // binary search org.apache.arrow.algorithm.search.VectorSearcher#binarySearch - O(log(n))
        // do search
        System.out.println("Binary search:");
        for (int i = 0; i < 10; i++) {
            int result = VectorSearcher.binarySearch(rawVector, comparatorInt, rawVector, i);
            System.out.println(result);
        }

        // negative case
        System.out.println(VectorSearcher.binarySearch(rawVector, comparatorInt, negVector, 0));

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
        // verify results
        System.out.println(vecToSort.getValueCount()==10);
        System.out.println("Sort the vector - In-place sorter:");
        System.out.println(vecToSort.isNull(0));
        System.out.println(vecToSort.isNull(1));
        System.out.println(2==vecToSort.get(2));
        System.out.println(8==vecToSort.get(3));
        System.out.println(10==vecToSort.get(4));
        System.out.println(10==vecToSort.get(5));
        System.out.println(12==vecToSort.get(6));
        System.out.println(17==vecToSort.get(7));
        System.out.println(23==vecToSort.get(8));
        System.out.println(35==vecToSort.get(9));

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
        // verify results
        System.out.println("Sort the vector - Out-of-place sorter:");
        System.out.println(vecOutOfPlaceSorter.getValueCount()==sortedVec.getValueCount());
        System.out.println(sortedVec.isNull(0));
        System.out.println(sortedVec.isNull(1));
        System.out.println(2==sortedVec.get(2));
        System.out.println(8==sortedVec.get(3));
        System.out.println(10==sortedVec.get(4));
        System.out.println(10==sortedVec.get(5));
        System.out.println(12==sortedVec.get(6));
        System.out.println(17==sortedVec.get(7));
        System.out.println(23==sortedVec.get(8));
        System.out.println(35==sortedVec.get(9));
    }
}
