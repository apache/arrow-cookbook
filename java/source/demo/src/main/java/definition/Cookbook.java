package definition;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class Cookbook {
    public static void main(String[] args) throws IOException {
        // create a column data type
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("A", "Id card");
        metadata.put("B", "Passport");
        metadata.put("C", "Visa");
        Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), null, metadata), null);

        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

        FieldType intType = new FieldType(true, new ArrowType.Int(32, true), /*dictionary=*/null);
        FieldType listType = new FieldType(true, new ArrowType.List(), /*dictionary=*/null);
        Field childField = new Field("intCol", intType, null);
        List<Field> childFields = new ArrayList<>();
        childFields.add(childField);
        Field points = new Field("points", listType, childFields);

        // create a definition
        Schema schemaPerson = new Schema(asList(name, document, age, points));

        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE); // deal with byte buffer allocation
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);

        // getting field vectors
        VarCharVector nameVectorOption1 = (VarCharVector) vectorSchemaRoot.getVector("name"); //interface FieldVector
        VarCharVector documentVectorOption1 = (VarCharVector) vectorSchemaRoot.getVector("document"); //interface FieldVector
        IntVector ageVectorOption1 = (IntVector) vectorSchemaRoot.getVector("age");
        ListVector pointsVectorOption1 = (ListVector) vectorSchemaRoot.getVector("points");

        // add values to the field vectors
        Util.setVector(nameVectorOption1, "david".getBytes(), "gladis".getBytes(), "juan".getBytes());
        Util.setVector(documentVectorOption1, "A".getBytes(), "B".getBytes(), "C".getBytes());
        Util.setVector(ageVectorOption1, 10,20,30);
        Util.setVector(pointsVectorOption1, asList(1,3,5,7,9), asList(2,4,6,8,10), asList(1,2,3,5,8));

        vectorSchemaRoot.setRowCount(3);

        // render data
        String initialContentToValidate = vectorSchemaRoot.contentToTSVString();
        System.out.println(initialContentToValidate);

        // render metadata
        System.out.println(documentVectorOption1.getField().getMetadata());

        // create an schema from json
        String jsonSchemaDefnition = schemaPerson.toJson();
        System.out.println(jsonSchemaDefnition);
        Schema schemaPersonFromJson = Schema.fromJSON(jsonSchemaDefnition);
        System.out.println(schemaPersonFromJson);

    }
}
