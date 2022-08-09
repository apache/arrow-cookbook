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

 * [Contributing to C++ Cookbook](cpp/CONTRIBUTING.md)
 * [Contributing to Java Cookbook](java/CONTRIBUTING.rst)
 * [Contributing to Python Cookbook](python/CONTRIBUTING.rst)
 * [Contributing to R Cookbook](r/CONTRIBUTING.md)

 ------------------------------------------------------------------------

All participation in the Apache Arrow project is governed by the Apache
Software Foundation’s [code of
conduct](https://www.apache.org/foundation/policies/conduct.html).
