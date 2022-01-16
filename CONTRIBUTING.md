# How to contribute to Apache Arrow Cookbook

## Did you find a bug?

If you find a bug in the cookbook, please let us know by opening an issue on GitHub.

## What makes a good recipe?

Recipes are solutions to specific problems that the user can copy/paste into their own code.
They answer specific questions that users might search for on Google or StackOverflow.
Some questions might be trivial, like how to read a CSV, while others might be more 
advanced; the complexity doesn't matter. If there is someone searching for that question,
the cookbook should have an answer for it.

Recipes should provide immediate instructions for how to perform a task, including a 
code example that can be copied and pasted. They do not need to explain the theoretical
knowledge behind the solution, but can instead link the relevant part of the Arrow user
guide and API documentation.

## Do you want to contribute new recipes or improvements?

We always welcome contributions of new recipes for the cookbook.  
To make a contributions, please fork this repo and submit a pull request with your contribution.

Any changes which add new code chunks or recipes must be tested when the `make test` command
is run, please refer to the language specific cookbook contribution documentation for information on
how to make your recipes testable.

 * [Contributing to Python Cookbook](python/CONTRIBUTING.rst)
 * [Contributing to R Cookbook](r/CONTRIBUTING.md)
 
 ------------------------------------------------------------------------

All participation in the Apache Arrow project is governed by the Apache
Software Foundation’s [code of
conduct](https://www.apache.org/foundation/policies/conduct.html).
