<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Bulding the C++ Cookbook

The C++ cookbook combines output from a set of C++ test programs with
an reStructuredText (RST) document tree rendered with Sphinx.

Running `make cpp` from the cookbook root directory (the one where
the `README.rst` exists) will compile the test code,
run the tests to generate the output, and will compile the cookbook
to HTML.

You will see the compiled result inside the `build/cpp` directory.

The above process requires conda to be installed and is primarily
intended for build systems. See below for more information on setting
up a development environment for developing recipes.

# Developing C++ Recipes

Every recipe is a combination of prose written in RST
format using the [Sphinx](https://www.sphinx-doc.org/) documentation
system and a snippet of a googletest test.

New recipes can be added to one of the existing `.rst` files if
they suit that section or you can create new sections by adding
additional `.rst` files in the `source` directory. You just
need to remember to add them to the `index.rst` file in the
`toctree` for them to become visible.

## Referencing a C++ Snippet

Most recipes will reference a snippet of C++ code. For simplicity
a custom `recipe` directive that can be used like so:

```
.. recipe:: ../code/creating_arrow_objects.cc CreatingArrays
  :caption: Creating an array from C++ primitives
  :dedent: 4
```

Each `recipe` directive has two required arguments. The first is
a path to the file containing the source file containing the snippet
and the second is the name of the snippet and must correspond to a
set of CreateRecipe/EndRecipe calls in the source file.

The directive will generate two code blocks in the cookbook. The first
code block will contain the source code itself and will be annotated
with any (optional) caption specified on the recipe directive. The
second block will contain the test output.

The optional `dedent` argument should be used to remove leading white
space from your source code.

## Writing a C++ Snippet

Each snippet source file contains a set of
[googletest](https://github.com/google/googletest) tests. Feel free to
use any googletest features needed to help setup and verify your test.
To reference a snippet you need to surround it in `BeginRecipe` and
`EndRecipe` calls. For example:

```
StartRecipe("CreatingArrays");
arrow::Int32Builder builder;
ASSERT_OK(builder.Append(1));
ASSERT_OK(builder.Append(2));
ASSERT_OK(builder.Append(3));
ASSERT_OK_AND_ASSIGN(shared_ptr<arrow::Array> arr, builder.Finish())
rout << arr->ToString() << endl;
EndRecipe("CreatingArrays");
```

The variable `rout` is set to a `std::ostream` instance that is used to
capture test output. Anything output to `rout` will show up in the recipe
output block when the recipe is rendered into the cookbook.

## Referencing Arrow C++ Documentation

The Arrow project has its own documentation for the C++ implementation that
is hosted at https://arrow.apache.org/docs/cpp/index.html. Fortunately,
this documentation is also built with Sphinx and so we can use the extension
`intersphinx` to reference sections of this documentation. To do so simply
write a standard Sphinx reference like so:

```
Typed subclasses of :cpp:class:`arrow::ArrayBuilder` make it easy
to efficiently create Arrow arrays from existing C++ data
```

A helpful command is
`python -msphinx.ext.intersphinx https://arrow.apache.org/docs/objects.inv`
which will list all of the possible targets to link to.

# Development Workflow

Running `make` at the top level can be rather slow as it will rebuild the
entire environment each time. It is primarily intended for use in CI and
requires you to have conda installed.

For recipe development you are encouraged to create your own out-of-source
cmake build. For example:

```
mkdir cpp/code/build
cd cpp/code/build
cmake ../code -DCMAKE_BUILD_TYPE=Debug
cmake --build .
ctest
```

Then you can rerun all of the tests with `ctest` and you can rebuild and
rerun individual tests much more quickly with something like
`cmake --build . --target creating_arrow_objects && ctest creating_arrow_objects`.
Everytime the cmake build is run it will update the recipe output file
referenced by the sphinx build so after rerunning a test you can visualize the
output by running `make html` in the `cpp` directory.

## Using Conda

If you are using conda then there is file `cpp/requirements.yml` which can be
used to create an environment for recipe development using the latest stable
Arrow version with the command:

```
conda env create -f cpp/environment.yml
```

There may be a conda-lock file available for your platform. Use this instead to
avoid having to perform the dependency resolution solve.

```
conda create -n cookbook-cpp --file cpp/conda-osx-arm64.lock
```

To update dependencies modify `cpp/requirements.yml` and then run

```
cd cpp
conda-lock --file environment.yml --kind explicit -p linux-aarch64 -p linux-64 -p osx-arm64
```

You can also create a conda environment to test your cookbooks against the Arrow Nightly
builds using the file `cpp/dev.yml`. Using the command:

```
conda env create -f cpp/dev.yml
```

This will create a conda environment called cookbook-cpp-dev instead.

# Development Philosophy

## Everything is the Cookbook

The entire document should serve as an example of how to use Arrow C++, not just the
referenced snippets. This means that the below style rules and guidelines apply to
source code that is not referenced by the cookbook itself.

## Style

This cookbook follows the same style rules as Arrow C++ which is the Google style
guide with a few exceptions described
[here](https://arrow.apache.org/docs/developers/cpp/development.html#code-style-linting-and-ci)

## Simple

The examples should be as simple as possible. If complex code (e.g. templates) can be
used to do something more efficiently then there should be a simple, inefficient version
alongside the more complex version.

Do not use `auto` in any of the templates unless you must (e.g. lambdas). Cookbook
viewers will be using a browser, not an IDE, and it is not always simple to determine
the inferred type.

# The Custom Recipe Directive

C++ is not, at the moment, a "notebook friendly" language and it does lend itself well
to being embedded inside an RST file. As such, we use a custom directive to link the
Googletest source files and the RST prose. The directive works with the helper methods
`BeginRecipe` and `EndRecipe` defined in `common.h`.

The helper method `BeginRecipe` will begin capturing output to `rout`. The helper method
`EndRecipe` will append the captured output and recipe name to string arrays. There is code
in `main.cc` which runs after the tests run to dump these arrays to a .arrow file (i.e. the
arrays will be serialized as a table using the Arrow IPC format).

When the sphinx build runs the directive `recipe` (defined in `cpp/ext`) will be loaded.
During this load the dataset of test outputs will be read. These test outputs will be used
whenever a recipe is referenced.

# Code of Conduct

All participation in the Apache Arrow project is governed by the Apache
Software Foundation’s
[code of conduct](https://www.apache.org/foundation/policies/conduct.html>).
