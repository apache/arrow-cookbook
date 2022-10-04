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

Building the Python Cookbook
===========================

The python cookbook uses the Sphinx documentation system.

Running ``make py`` from the cookbook root directory (the one where
the ``README.rst`` exists) will install all necessary dependencies
and will compile the cookbook to HTML.

You will see the compiled result inside the ``build/py`` directory.

Testing Python Recipes
======================

All recipes in the cookbook must be tested. The cookbook uses
``doctest`` to verify the recipes.

Running ``make pytest`` from the cookbook root directory
will verify that the code for all the recipes runs correctly
and provides the expected output.

If the environment variable ``ARROW_NIGHTLY`` is set to ``1``
the cookbooks will be run against the latest development version of
Arrow published by the `Nightly jobs. <https://arrow.apache.org/docs/python/install.html#installing-nightly-packages>`_

Example to run the cookbooks using the nightly jobs:

```
$ ARROW_NIGHTLY=1
$ make pytest
```

Adding Python Recipes
=====================

The recipes are written in **reStructuredText** format using 
the `Sphinx <https://www.sphinx-doc.org/>`_ documentation system.

New recipes can be added to one of the existing ``.rst`` files if
they suit that section or you can create new sections by adding
additional ``.rst`` files in the ``source`` directory. You just
need to remember to add them to the ``index.rst`` file in the
``toctree`` for them to become visible.

The only requirement for recipes is that each code block in the recipe 
must be written using the ``.. testcode::`` directive, 
so that it can get tested.

If the code block changes, alters or creates data, the recipe should
``print`` the data to show how it changed and have a ``.. testoutput::``
directive to confirm that the printed data is the expected one.

For example a new recipe about how to create an Arrow array
might look like:

.. code-block::

    You can create a new :class:`pyarrow.Array` providing the
    data for the array through the :meth:`pyarrow.array` factory function

    .. testcode::

        import pyarrow as pa
        array = pa.array(range(5))
        print(array)

    .. testoutput::

        [
          0,
          1,
          2,
          3,
          4
        ]

If you refer to any ``pyarrow`` class, function or method using
``:class:``, ``:meth:`` or ``:func:`` directives a link to their
documentation in the pyarrow API reference will be automatically
created.

------------------------------------------------------------------------

All participation in the Apache Arrow project is governed by the Apache
Software Foundation’s 
`code of conduct <https://www.apache.org/foundation/policies/conduct.html>`_.
