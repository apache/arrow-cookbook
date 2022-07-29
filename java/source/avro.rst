===============================
Converting Avro format to Arrow
===============================

.. contents::

Avro to Arrow
=============

.. testcode::

    import org.apache.arrow.AvroToArrow;
    import org.apache.arrow.AvroToArrowConfig;
    import org.apache.arrow.AvroToArrowConfigBuilder;
    import org.apache.arrow.AvroToArrowVectorIterator;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.avro.Schema;
    import org.apache.avro.io.BinaryDecoder;
    import org.apache.avro.io.DecoderFactory;

    import java.io.File;
    import java.io.FileInputStream;

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    AvroToArrowConfig config = new AvroToArrowConfigBuilder(allocator).build();

    BinaryDecoder decoder = new DecoderFactory().binaryDecoder(new FileInputStream("./thirdpartydeps/avro/users.avro"), null);

    Schema schema = new Schema.Parser().parse(new File("./thirdpartydeps/avro/user.avsc"));
    AvroToArrowVectorIterator avroToArrowVectorIterator = AvroToArrow.avroToArrowIterator(schema, decoder, config);

    while(avroToArrowVectorIterator.hasNext()) {
        VectorSchemaRoot root = avroToArrowVectorIterator.next();
        System.out.print(root.contentToTSVString());
    }

.. testoutput::

    name    favorite_number    favorite_color
    Alyssa    256    null
    Ben    7    red