Apache Arrow Cookbooks
======================

Cookbooks are a collection of recipes about common tasks
that Arrow users might want to do. The cookbook is actually
composed of multiple cookbooks, one for each supported platform,
that contain the recipes for that specific platform.

All cookbooks are buildable to HTML and verifiable by running
a set of tests that confirm that the recipes are still working
as expected.

Each cookbook is implemented using platform specific tools.
For this reason a Makefile is provided which abstracts platform
specific concerns and makes it possible to build/test all cookbooks
without any platform specific knowledge (as long as dependencies
are available on the target system).

Building All Cookbooks
----------------------

``make all``

Testing All Cookbooks
---------------------

``make test``

Listing Available Commands
--------------------------

``make help``

Building Platform Specific Cookbook
-----------------------------------

Refer to ``make help`` to learn the
commands that build or test the cookbook for the platform you
are targeting.

Prerequisites
=============

Both the R and Python cookbooks will try to install the
dependencies they need (including latests pyarrow/arrow-R version).
This means that as far as you have a working Python/R environment
able to install dependencies through the respective package manager
you shouldn't need to install anything manually.

Contributing to the Cookbook
============================

Please refer to the `CONTRIBUTING.md <CONTRIBUTING.md>`_ file
for instructions about how to contribute to the Apache Arrow Cookbook.

------------------------------------------------------------------------

All participation in the Apache Arrow project is governed by the Apache
Software Foundation’s 
`code of conduct <https://www.apache.org/foundation/policies/conduct.html>`_.
