.. _arrow-python-java:

========================
PyArrow Java Integration
========================

The PyArrow library offers a powerful API for Python that can be integrated with Java applications.
This document provides a guide on how to enable seamless data exchange between Python and Java components using PyArrow.

.. contents::

Dictionary Data Roundtrip
=========================

    This section demonstrates a data roundtrip, where a dictionary array is created in Python, accessed and updated in Java,
    and finally re-accessed and validated in Python for data consistency.


Python Component:
-----------------

    The Python code uses jpype to start the JVM and make the Java class MapValuesConsumer available to Python.
    Data is generated in PyArrow and exported through C Data to Java.

.. code-block:: python

    import jpype
    import jpype.imports
    from jpype.types import *
    import pyarrow as pa
    from pyarrow.cffi import ffi as arrow_c

    # Init the JVM and make MapValuesConsumer class available to Python.
    jpype.startJVM(classpath=[ "../target/*"])
    java_c_package = jpype.JPackage("org").apache.arrow.c
    MapValuesConsumer = JClass('MapValuesConsumer')
    CDataDictionaryProvider = JClass('org.apache.arrow.c.CDataDictionaryProvider')

    # Starting from Python and generating data

    # Create a Python DictionaryArray

    dictionary = pa.dictionary(pa.int64(), pa.utf8())
    array = pa.array(["A", "B", "C", "A", "D"], dictionary)
    print("From Python")
    print("Dictionary Created: ", array)

    # create the CDataDictionaryProvider instance which is
    # required to create dictionary array precisely
    c_provider = CDataDictionaryProvider()

    consumer = MapValuesConsumer(c_provider)

    # Export the Python array through C Data
    c_array = arrow_c.new("struct ArrowArray*")
    c_array_ptr = int(arrow_c.cast("uintptr_t", c_array))
    array._export_to_c(c_array_ptr)

    # Export the Schema of the Array through C Data
    c_schema = arrow_c.new("struct ArrowSchema*")
    c_schema_ptr = int(arrow_c.cast("uintptr_t", c_schema))
    array.type._export_to_c(c_schema_ptr)

    # Send Array and its Schema to the Java function
    # that will update the dictionary
    consumer.update(c_array_ptr, c_schema_ptr)

    # Importing updated values from Java to Python

    # Export the Python array through C Data
    updated_c_array = arrow_c.new("struct ArrowArray*")
    updated_c_array_ptr = int(arrow_c.cast("uintptr_t", updated_c_array))

    # Export the Schema of the Array through C Data
    updated_c_schema = arrow_c.new("struct ArrowSchema*")
    updated_c_schema_ptr = int(arrow_c.cast("uintptr_t", updated_c_schema))

    java_wrapped_array = java_c_package.ArrowArray.wrap(updated_c_array_ptr)
    java_wrapped_schema = java_c_package.ArrowSchema.wrap(updated_c_schema_ptr)

    java_c_package.Data.exportVector(
        consumer.getAllocatorForJavaConsumer(),
        consumer.getVector(),
        c_provider,
        java_wrapped_array,
        java_wrapped_schema
    )

    print("From Java back to Python")
    updated_array = pa.Array._import_from_c(updated_c_array_ptr, updated_c_schema_ptr)

    # In Java and Python, the same memory is being accessed through the C Data interface.
    # Since the array from Java and array created in Python should have same data. 
    assert updated_array.equals(array)
    print("Updated Array: ", updated_array)

    del updated_array

.. code-block:: shell

    From Python
    Dictionary Created:
    -- dictionary:
    [
        "A",
        "B",
        "C",
        "D"
    ]
    -- indices:
    [
        0,
        1,
        2,
        0,
        3
    ]
    Doing work in Java
    From Java back to Python
    Updated Array:
    -- dictionary:
    [
        "A",
        "B",
        "C",
        "D"
    ]
    -- indices:
    [
        2,
        1,
        2,
        0,
        3
    ]

In the Python component, the following steps are executed to demonstrate the data roundtrip:

1. Create data in Python 
2. Export data to Java
3. Import updated data from Java
4. Validate the data consistency


Java Component:
---------------

    In the Java component, the MapValuesConsumer class receives data from the Python component through C Data. 
    It then updates the data and sends it back to the Python component.

.. testcode::

    import org.apache.arrow.c.ArrowArray;
    import org.apache.arrow.c.ArrowSchema;
    import org.apache.arrow.c.Data;
    import org.apache.arrow.c.CDataDictionaryProvider;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.FieldVector;
    import org.apache.arrow.vector.BigIntVector;
    import org.apache.arrow.util.AutoCloseables;


    class MapValuesConsumer implements AutoCloseable {
        private final BufferAllocator allocator;
        private final CDataDictionaryProvider provider;
        private FieldVector vector;
        private final BigIntVector intVector;


        public MapValuesConsumer(CDataDictionaryProvider provider, BufferAllocator allocator) {
            this.provider = provider;
            this.allocator = allocator;
            this.intVector = new BigIntVector("internal_test_vector", allocator);
        }

        public BufferAllocator getAllocatorForJavaConsumer() {
            return allocator;
        }

        public FieldVector getVector() {
            return this.vector;
        }

        public void update(long c_array_ptr, long c_schema_ptr) {
            ArrowArray arrow_array = ArrowArray.wrap(c_array_ptr);
            ArrowSchema arrow_schema = ArrowSchema.wrap(c_schema_ptr);
            this.vector = Data.importVector(allocator, arrow_array, arrow_schema, this.provider);
            this.doWorkInJava(vector);
        }

        public FieldVector updateFromJava(long c_array_ptr, long c_schema_ptr) {
            ArrowArray arrow_array = ArrowArray.wrap(c_array_ptr);
            ArrowSchema arrow_schema = ArrowSchema.wrap(c_schema_ptr);
            this.vector = Data.importVector(allocator, arrow_array, arrow_schema, this.provider);
            this.doWorkInJava(vector);
            return vector;
        }

        private void doWorkInJava(FieldVector vector) {
            System.out.println("Doing work in Java");
            BigIntVector bigIntVector = (BigIntVector)vector;
            bigIntVector.setSafe(0, 2);
        }

        public BigIntVector getIntVectorForJavaConsumer() {
            intVector.allocateNew(3);
            intVector.set(0, 1);
            intVector.set(1, 7);
            intVector.set(2, 93);
            intVector.setValueCount(3);
            return intVector;
        }

        @Override
        public void close() throws Exception {
            AutoCloseables.close(intVector);
        }
    }
    try (BufferAllocator allocator = new RootAllocator()) {
        CDataDictionaryProvider provider = new CDataDictionaryProvider();
        try (final MapValuesConsumer mvc = new MapValuesConsumer(provider, allocator)) {
            try (
            ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator)
            )  {
                    Data.exportVector(allocator, mvc.getIntVectorForJavaConsumer(), provider, arrowArray, arrowSchema);
                    FieldVector updatedVector = mvc.updateFromJava(arrowArray.memoryAddress(), arrowSchema.memoryAddress());
                    try (ArrowArray usedArray = ArrowArray.allocateNew(allocator);
                        ArrowSchema usedSchema = ArrowSchema.allocateNew(allocator)) {
                        Data.exportVector(allocator, updatedVector, provider, usedArray, usedSchema);
                        try(FieldVector valueVectors = Data.importVector(allocator, usedArray, usedSchema, provider)) {
                            System.out.println(valueVectors);
                        }
                    }
                    updatedVector.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    } catch (Exception ex) {
        ex.printStackTrace();
    }


.. testoutput::

    Doing work in Java
    [2, 7, 93]


The Java component performs the following actions:

1. Receives data from the Python component.
2. Updates the data.
3. Exports the updated data back to Python.

By integrating PyArrow in Python and Java components, this example demonstrates that 
a system can be created where data is shared and updated across both languages seamlessly.
