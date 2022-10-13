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

Building the Java Cookbook
=========================
The Java cookbook uses the Sphinx documentation system.

Dependencies
-------------------------
The following are required to successfully build the Java cookbook:

Python
^^^^^^^^^^^^^^^^^^^^^^^^^
The cookbook build tooling depends upon Python, and the ability to
install needed packages via pip, to build the Java cookbook.  The
dependency packages managed via pip by build scripts are found at
`requirements.txt <requirements.txt>`_.

Java Shell
^^^^^^^^^^^^^^^^^^^^^^^^^
For Java cookbook we are running these with Java Shell tool -
`JShell <https://docs.oracle.com/en/java/javase/11/jshell/introduction-jshell.html>`_

.. code-block:: bash

    > java --version
    java 11.0.14 2022-01-18 LTS
    Java(TM) SE Runtime Environment 18.9 (build 11.0.14+8-LTS-263)

.. code-block:: bash

    > jshell --version
    jshell 11.0.14


Build Process
-------------------------
Run ``make java`` from the cookbook root directory (the one where
the ``README.rst`` exists) to install all necessary dependencies
and compile the cookbook to HTML.

You will see the compiled result inside the ``build/java`` directory.

.. code-block:: bash

   $ cd arrow-cookbook
   $ make java
     ...
     build succeeded.
     ...
     The HTML pages are in build/html.
     mkdir -p build/java
     cp -r java/build/html/* build/java

If the environment variable ``ARROW_NIGHTLY`` is defined and not 0
the cookbooks will be run against the latest development version of
Arrow published by the `Nightly jobs. <https://arrow.apache.org/docs/java/install.html#installing-nightly-packages>`_

Testing Java Recipes
====================

All recipes in the cookbook must be tested. The cookbook uses
``javadoctest`` to verify the recipes.

Run ``make javatest`` from the cookbook root directory
to verify that the code for all the recipes runs correctly
and provides the expected output.

.. code-block:: bash

   $ cd arrow-cookbook
   $ # SDK 11+
   $ java --version
   $ jshell --version
   $ make javatest
     ...
     1 items passed all tests:
        7 tests in default
     7 tests in 1 items.
     7 passed and 0 failed.
     Test passed.
     ...
     Doctest summary
     ===============
        44 tests
         0 failures in tests
         0 failures in setup code
         0 failures in cleanup code
     build succeeded.

Please note: In case you need to test the recipes with nightly version
please export environment variable needed:

.. code-block:: bash

   $ export ARROW_NIGHTLY=1
   $ echo $ARROW_NIGHTLY
     1
   $ make javatest

Adding Java Recipes
===================

The recipes are written in **reStructuredText** format using 
the `Sphinx <https://www.sphinx-doc.org/>`_ documentation system.

New recipes can be added to one of the existing ``.rst`` files if
they suit that section or you can create new sections by adding
additional ``.rst`` files in the ``source`` directory. You just
need to remember to add them to the ``index.rst`` file in the
``toctree`` for them to become visible.

Java Sphinx Directive
=====================

To support testing java code documentation a sphinx directive
was created: ext/javadoctest.py

Execute validations before commit sphinx directive extension:

Format code (before committing)

.. code-block:: bash

    > cd java/ext
    > black javadoctest.py

Sort imports (before committing)

.. code-block:: bash

    > cd java/ext
    > isort javadoctest.py

Lint code (before committing)

.. code-block:: bash

    > cd java/ext
    > flake8
------------------------------------------------------------------------

All participation in the Apache Arrow project is governed by the Apache
Software Foundation’s 
`code of conduct <https://www.apache.org/foundation/policies/conduct.html>`_.
