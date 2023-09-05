.. _arrow-python-java:

========================
PyArrow Java Integration
========================

PyArrow for Python is a strong API which is useful for certain Java application development.

.. contents::

Dictionary Data Roundtrip
=========================

    To demonstrate how dictionary data can be used in both Python and Java, consider the following.

    Data is created in Python, then it will be accessed in Java and data will be updated. updated
    data will be again used in Python and validated for consistency through out the entire application.

Python Component:
-----------------

.. code-block:: python

    import jpype
    import jpype.imports
    from jpype.types import *
    import pyarrow as pa
    from pyarrow.cffi import ffi as arrow_c

    # Init the JVM and make MapValuesV2 class available to Python.
    jpype.startJVM(classpath=[ "../arrow-java-playground/target/*"])
    java_c_package = jpype.JPackage("org").apache.arrow.c
    MapValuesConsumer = JClass('io.arrow.playground.python.MapValuesConsumer')
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

In Python component, the following steps are taken to demonstrate the roundtrip process.

1. Create Data in Python 
2. Access Data from Java
3. Update Data from Java
4. Access Data from Python
5. Validate the data change


Java Component:
---------------

.. code-block:: java

    import org.apache.arrow.c.ArrowArray;
    import org.apache.arrow.c.ArrowSchema;
    import org.apache.arrow.c.Data;
    import org.apache.arrow.c.CDataDictionaryProvider;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.FieldVector;
    import org.apache.arrow.vector.BigIntVector;


    public class MapValuesConsumer {
        private final static BufferAllocator allocator = new RootAllocator();
        private final CDataDictionaryProvider provider;
        private FieldVector vector;

        public MapValuesConsumer(CDataDictionaryProvider provider) {
            this.provider = provider;
        }

        public static BufferAllocator getAllocatorForJavaConsumer() {
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

        private void doWorkInJava(FieldVector vector) {
            System.out.println("Doing work in Java");
            BigIntVector bigIntVector = (BigIntVector)vector;
            bigIntVector.setSafe(0, 2);
        }
    }

Java component access the data from Python and update the vector,
and this is later accessed in Python component. 
