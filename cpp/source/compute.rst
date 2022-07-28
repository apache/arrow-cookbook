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

====================================
Defining and Using Compute Functions
====================================

This section contains (or will contain) a number of recipes illustrating how to
define new "compute functions" or how to use existing ones. Arrow contains a "Compute
API," which primarily consists of a "registry" of functions that can be invoked.
Currently, Arrow populates a default registry with a variety of useful functions. The
recipes provided in this section show some approaches to define a compute function as well
as how to invoke a compute function by name, given a registry.


.. contents::

Invoke a Compute Function
=========================

When invoking a compute function, the function must exist in a function registry. In this
recipe, we use `CallFunction()` to invoke the function with name "named_scalar_fn".

.. recipe:: ../code/compute_fn.cc InvokeByCallFunction
  :caption: Use CallFunction() to invoke a compute function by name
  :dedent: 2

.. note::
    This method allows us to specify arguments as a vector and a custom ExecContext.

If an `ExecContext` is not passed to `CallFunction` (it is null), then the default
FunctionRegistry will be used to call the function from.

If we have defined a convenience function that wraps `CallFunction()`, then we can call
that function instead. Various compute functions provided by Arrow have these convenience
functions defined, such as `Add` or `Subtract`.

.. recipe:: ../code/compute_fn.cc InvokeByConvenienceFunction
  :caption: Use a convenience invocation function to call a compute function
  :dedent: 2


Adding a Custom Compute Function
================================

To make a custom compute function available, there are 3 primary steps:

1. Define kernels for the function (these implement the actual logic)

2. Associate the kernels with a function object

3. Add the function object to a function registry


Define Function Kernels
-----------------------

A kernel is a particular function that implements desired logic for a compute function.
There are at least a couple of types of function kernels, such as initialization kernels
and execution kernels. An initialization kernel prepares the initial state of a compute
function, while an execution kernel executes the main processing logic of the compute
function. The body of a function kernel may use other functions, but the kernel function
itself is a singular instance that will be associated with the desired compute function.
While compute functions can be associated with an initialization and execution kernel
pair, this recipe only shows the definition of an execution kernel.

The signature of an execution kernel is relatively standardized: it returns a `Status` and
takes a context, some arguments, and a pointer to an output result. The context wraps an
`ExecContext` and other metadata about the environment in which the kernel function should
be executed. The input arguments are contained within an `ExecSpan` (newly added in place
of `ExecBatch`), which holds non-owning references to argument data. Finally, the
`ExecResult` pointed to should be set to an appropriate `ArraySpan` or `ArrayData`
instance, depending on ownership semantics of the kernel's output.

.. recipe:: ../code/compute_fn.cc DefineAComputeKernel
  :caption: Define an example compute kernel that uses ScalarHelper from hashing.h to hash
            input values
  :dedent: 2

This recipe shows basic validation of `input_arg` which contains a vector of input
arguments. Then, the input `Array` is accessed from `input_arg` and a `Buffer` is
allocated to hold output results. After the main loop is completed, the allocated `Buffer`
is wrapped in an `ArrayData` instance and referenced by `out`.


Associate Kernels with a Function
---------------------------------

The process of adding kernels to a compute function is easy: (1) create an appropriate
`Function` instance--`ScalarFunction` in this case--and (2) call the `AddKernel` function.
The more difficult part of this process is repeating for the desired data types and
knowing how the signatures work.

.. recipe:: ../code/compute_fn.cc AddKernelToFunction
  :caption: Instantiate a ScalarFunction and add our execution kernel to it
  :dedent: 2

A `ScalarFunction` represents a "scalar" or "element-wise" compute function (see
documentation on the Compute API). The signature used in this recipe passes:

1. A function name (to be used when calling it)

2. An "Arity" meaning how many input arguments it takes (like cardinality)

3. A `FunctionDoc` instance (to associate some documentation programmatically)

Then, `AddKernel` expects:

1. A vector of data types for each input argument

2. An output data type for the result

3. The function to be used as the execution kernel

4. The function to be used as the initialization kernel (optional)

Note that the constructor for `ScalarFunction` is more interested in how many arguments to
expect, and some information about the compute function itself; whereas, the function to
add a kernel specifies data types and the functions to call at runtime.


Add Function to Registry
------------------------

Finally, adding the function to a registry is wonderfully straightforward.

.. recipe:: ../code/compute_fn.cc AddFunctionToRegistry
  :caption: Use convenience function to get a ScalarFunction with associated kernels, then
            add it to the given FunctionRegistry
  :dedent: 2

In this recipe, we simply wrap the logic in a convenience function that: (1) creates a
`ScalarFunction`, (2) adds our execution kernel to the compute function, and (3) returns
the compute function. Then, we add the compute function to some registry. This recipe
takes the `FunctionRegistry` as an argument so that it is easy to call from the same place
that the Arrow codebase registers other provided functions. Otherwise, we can add our
compute function to the default registry, or a custom registry.
