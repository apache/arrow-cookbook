package create;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;

import static java.util.Arrays.asList;

public class Cookbook {
    public static void main(String[] args) {
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

        // create int vector
        IntVector intVector = new IntVector("intVector", rootAllocator);
        Util.setVector(intVector, 1,2,3);
        System.out.println(intVector);

        // create a varchar vector
        VarCharVector varcharVector = new VarCharVector("varcharVector", rootAllocator);
        Util.setVector(varcharVector, "david".getBytes(), "gladis".getBytes(), "juan".getBytes());
        System.out.println(varcharVector);

        // create a list vector
        ListVector listVector = ListVector.empty("listVector", rootAllocator);
        Util.setVector(listVector, asList(1,3,5,7,9), asList(2,4,6,8,10), asList(1,2,3,5,8));
        System.out.println(listVector);
    }
}
