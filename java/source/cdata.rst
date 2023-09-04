.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

================
C Data Interface
================

Recipes related to how to exchange data using C Data Interface specification.

.. contents::

Python (Consumer) - Java (Producer)
===================================

    For Python Consumer and Java Producer, please consider:

    - The Root Allocator should be shared for all memory allocations.

    - The Python application will sometimes shut down the Java JVM but Java JNI C Data will still work on releasing exported objects, which is why some guards have been implemented to protect against such scenarios. A warning message "WARNING: Failed to release Java C Data resource" indicates this scenario.

    - We do not know when Root Allocator will be closed. It is for this reason that the Root Allocator should survive so long as the export/import of used objects is released. Here is an example of this scenario:

        + Whenever Java code calls `allocator.close`, a memory leak will occur since many objects will have to be released on either Python or Java JNI sides.

        + To solve memory leak problems, you will call Java `allocator.close` when Python and Java JNI have released all their objects, which is impossible to accomplish.

    - In addition, Java applications should expose a method for closing all Java-created objects independently from Root Allocators.


Sharing ValueVector
*******************

Java Side:

.. testcode::

    import org.apache.arrow.c.ArrowArray;
    import org.apache.arrow.c.ArrowSchema;
    import org.apache.arrow.c.Data;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;
    import org.apache.arrow.vector.FieldVector;
    import org.apache.arrow.vector.IntVector;

    final static BufferAllocator allocator = new RootAllocator();
    final static IntVector intVector =
        new IntVector("to_be_consumed_by_python", allocator);

    public static BufferAllocator getAllocatorForJavaConsumers() {
      return allocator;
    }

    public static IntVector getIntVectorForJavaConsumers() {
      intVector.allocateNew(3);
      intVector.set(0, 1);
      intVector.set(1, 7);
      intVector.set(2, 93);
      intVector.setValueCount(3);
      return intVector;
    }

    public static void closeAllocatorForJavaConsumers() {
      try {
        AutoCloseables.close(AutoCloseables.iter(intVector));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public static void simulateAsAJavaConsumers() {
      ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
      ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
      Data.exportVector(allocator, getIntVectorForJavaConsumers(), null, arrowArray, arrowSchema);
      FieldVector valueVectors = Data.importVector(allocator, arrowArray, arrowSchema, null);
      System.out.println(valueVectors);
      closeAllocatorForJavaConsumers();
      allocator.close();
    }

    simulateAsAJavaConsumers();

.. testoutput::

   [1, 7, 93]

Python Side:

.. code-block:: python

    import jpype
    import pyarrow as pa
    from pyarrow.cffi import ffi

    jvmargs=["-Darrow.memory.debug.allocator=true"]
    jpype.startJVM(*jvmargs, jvmpath=jpype.getDefaultJVMPath(), classpath=[
        "./target/java-python-by-cdata-1.0-SNAPSHOT-jar-with-dependencies.jar"])
    java_value_vector_api = jpype.JClass('ShareValueVectorAPI')
    java_c_package = jpype.JPackage("org").apache.arrow.c
    py_c_schema = ffi.new("struct ArrowSchema*")
    py_ptr_schema = int(ffi.cast("uintptr_t", py_c_schema))
    py_c_array = ffi.new("struct ArrowArray*")
    py_ptr_array = int(ffi.cast("uintptr_t", py_c_array))
    java_wrapped_schema = java_c_package.ArrowSchema.wrap(py_ptr_schema)
    java_wrapped_array = java_c_package.ArrowArray.wrap(py_ptr_array)
    java_c_package.Data.exportVector(
        java_value_vector_api.getAllocatorForJavaConsumers(),
        java_value_vector_api.getIntVectorForJavaConsumers(),
        None,
        java_wrapped_array,
        java_wrapped_schema
    )
    py_array = pa.Array._import_from_c(py_ptr_array, py_ptr_schema)
    print(type(py_array))
    print(py_array)

.. code-block:: shell

    <class 'pyarrow.lib.Int32Array'>
    [
      1,
      7,
      93
    ]



