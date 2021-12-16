import org.apache.arrow.algorithm.search.VectorSearcher;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.StableVectorComparator;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compare.TypeEqualsVisitor;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class Data {
    public static void main(String[] args) {
        // comparing vector fields org.apache.arrow.vector.compare.VectorValueEqualizer on vector module
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE); // deal with byte buffer allocation
        IntVector right = new IntVector("int", rootAllocator);
        IntVector left1 = new IntVector("int", rootAllocator);
        IntVector left2 = new IntVector("int2", rootAllocator);

        setVector(right, 10,20,30);

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
        VectorValueComparator<VarCharVector> comparatorValues = new TestVarCharSorter(); // less than, equal to, greater than
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
        rawVector.allocateNew(100);
        rawVector.setValueCount(100);
        negVector.allocateNew(1);
        negVector.setValueCount(1);
        for (int i = 0; i < 100; i++) { // prepare data in sorted order
            if (i == 0) {
                rawVector.setNull(i);
            } else {
                rawVector.set(i, i);
            }
        }
        negVector.set(0, -333);
        VectorValueComparator<IntVector> comparatorInt = DefaultVectorComparators.createDefaultComparator(rawVector); // do search
        System.out.println("Linear search:");
        for (int i = 0; i < 100; i++) {
            int result = VectorSearcher.linearSearch(rawVector, comparatorInt, rawVector, i);
            System.out.println(result);
        }
        // negative case
        System.out.println(VectorSearcher.linearSearch(rawVector, comparatorInt, negVector, 0));

        // binary search org.apache.arrow.algorithm.search.VectorSearcher#binarySearch - O(log(n))
        // do search
        System.out.println("Binary search:");
        for (int i = 0; i < 100; i++) {
            int result = VectorSearcher.binarySearch(rawVector, comparatorInt, rawVector, i);
            System.out.println(result);
        }

        // negative case
        System.out.println(VectorSearcher.binarySearch(rawVector, comparatorInt, negVector, 0));

    }

    public static void setVector(IntVector vector, Integer... values) {
        final int length = values.length;
        vector.allocateNew(length);
        for (int i = 0; i < length; i++) {
            if (values[i] != null) {
                vector.set(i, values[i]);
            }
        }
        vector.setValueCount(length);
    }

    public static void setVector(VarCharVector vector, byte[]... values) {
        final int length = values.length;
        vector.allocateNewSafe();
        for (int i = 0; i < length; i++) {
            if (values[i] != null) {
                vector.set(i, values[i]);
            }
        }
        vector.setValueCount(length);
    }

    public static void setVector(ListVector vector, List<Integer>... values) {
        vector.allocateNewSafe();
        Types.MinorType type = Types.MinorType.INT;
        vector.addOrGetVector(FieldType.nullable(type.getType()));

        IntVector dataVector = (IntVector) vector.getDataVector();
        dataVector.allocateNew();

        // set underlying vectors
        int curPos = 0;
        vector.getOffsetBuffer().setInt(0, curPos);
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                BitVectorHelper.unsetBit(vector.getValidityBuffer(), i);
            } else {
                BitVectorHelper.setBit(vector.getValidityBuffer(), i);
                for (int value : values[i]) {
                    dataVector.setSafe(curPos, value);
                    curPos += 1;
                }
            }
            vector.getOffsetBuffer().setInt((i + 1) * BaseRepeatedValueVector.OFFSET_WIDTH, curPos);
        }
        dataVector.setValueCount(curPos);
        vector.setLastSet(values.length - 1);
        vector.setValueCount(values.length);
    }

    /**
     * Utility comparator that compares varchars by the first character.
     */
    private static class TestVarCharSorter extends VectorValueComparator<VarCharVector> {
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
}
