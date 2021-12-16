import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;
import static java.util.Arrays.asList;

public class Create {
    public static void main(String[] args) {
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

        // create int vector
        IntVector intVector = new IntVector("intVector", rootAllocator);
        setVector(intVector, 1,2,3);
        System.out.println(intVector);

        // create a varchar vector
        VarCharVector varcharVector = new VarCharVector("varcharVector", rootAllocator);
        setVector(varcharVector, "david".getBytes(), "gladis".getBytes(), "juan".getBytes());
        System.out.println(varcharVector);

        // create a list vector
        ListVector listVector = ListVector.empty("listVector", rootAllocator);
        setVector(listVector, asList(1,3,5,7,9), asList(2,4,6,8,10), asList(1,2,3,5,8));
        System.out.println(listVector);
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
}
