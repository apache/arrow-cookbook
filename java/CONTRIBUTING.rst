Bulding the Java Cookbook
=========================

The Java cookbook uses the Sphinx documentation system.

Running ``make java`` from the cookbook root directory (the one where
the ``README.rst`` exists) will install all necessary dependencies
and will compile the cookbook to HTML.

For java cookbook we are running these with Java Shell tool -
`JShell <https://docs.oracle.com/en/java/javase/11/jshell/introduction-jshell.html>`_

.. code-block:: bash

    > java --version
    java 11.0.14 2022-01-18 LTS
    Java(TM) SE Runtime Environment 18.9 (build 11.0.14+8-LTS-263)

.. code-block:: bash

    > jshell --version
    jshell 11.0.14

You will see the compiled result inside the ``build/java`` directory.

Testing Java Recipes
====================

All recipes in the cookbook must be tested. The cookbook uses
``javadoctest`` to verify the recipes.

Running ``make javatest`` from the cookbook root directory
will verify that the code for all the recipes runs correctly
and provides the expected output.

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
