package data;

import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;

public class Util {
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

    /**
     * Utility comparator that compares varchars by the first character.
     */
    public static class TestVarCharSorter extends VectorValueComparator<VarCharVector> {
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
