package definition;

import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;

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
