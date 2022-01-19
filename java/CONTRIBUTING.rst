Bulding the Java Cookbook
=========================

The Java cookbook uses the Sphinx documentation system.

Running ``make java`` from the cookbook root directory (the one where
the ``README.rst`` exists) will install all necessary dependencies
and will compile the cookbook to HTML.

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

------------------------------------------------------------------------

All participation in the Apache Arrow project is governed by the Apache
Software Foundation’s 
`code of conduct <https://www.apache.org/foundation/policies/conduct.html>`_.
