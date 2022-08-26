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

# Contributing

We actively welcome contributions to the Arrow R cookbook!  If you want to make a contribution, please fork this repo and make a pull request with your changes.  If you see any errors or have suggestions for recipes you'd like to see but do not know how to create, please open a GitHub issue.

# Fixing Typos

If you find any typos or minor issues in the cookbook, feel free to submit a PR without opening an issue first.

# Adding R Recipes

The recipes are written in RMarkdown format using `bookdown`.

You can add new recipes to one of the existing ``.Rmd`` files, or you can create new sections by adding additional ``.Rmd`` files in the `content` directory.  If you add a new file, you should add it to the `rmd_files` list in `content/_bookdown.yml` for it to be visible in the rendered cookbook.

After each code chunk in the recipe, you should add a test chunk that tests that the code chunk's output is as expected.  Using a test chunk will allow your recipe to be tested against the latest version of arrow, and make it easier to detect if any changes made to arrow result in your recipe becoming out-of-date.

Each significant code chunk must be given a descriptive label, and be immediately followed by a unit test of its output.  This test should be labelled "test_" followed by the name of the chunk that it is testing.  The test chunk should also have the `opts.label` attribute set to "test" - this will ensure that the test is not rendered as part of the cookbook.

Here's an example of a recipe and a test:

~~~
```{r, write_parquet}
# Create table
my_table <- arrow_table(tibble::tibble(group = c("A", "B", "C"), score = c(99, 97, 99)))

# Write to Parquet
write_parquet(my_table, "my_table.parquet")
```

```{r, test_write_parquet, opts.label = "test"}
test_that("write_parquet chunk works as expected", {
  expect_true(file.exists("my_table.parquet"))
})
```
~~~

# Testing R Recipes

All recipes in the cookbook must be tested. The cookbook uses `testthat` to verify the recipes.

Running ``make rtest`` from the cookbook root directory will verify that the code for all of the R recipes run correctly and provide the expected output.

# Building the Arrow R Cookbook

The Arrow R cookbook has been written using `bookdown`.

Running ``make r`` from the cookbook root directory (the one where the ``Makefile`` exists) will install all necessary dependencies and compile the cookbook to HTML.

You can see the compiled result inside the ``build/r`` directory.

If you add a new recipe to the cookbook, you do not need to commit changes to `build/r` to the repo, as this is automatically run by our CI when building the latest version of the cookbook on the main branch.

The dependencies installed include: `testthat`, `bookdown`, `knitr`, `purrr`, `remotes`, `dplyr`, `stringr`, `reticulate` and the latest nightly build of the Arrow R package `arrow`. `bookdown` requires `pandoc` to build the site---`pandoc` ships with [RStudio](https://www.rstudio.com/products/rstudio/), or you can [install it independently](https://pandoc.org/installing.html). If `bookdown` is using RStudio's `pandoc` you may need to set the path globally in your zsh/bash start up file (e.g. `export RSTUDIO_PANDOC=/Applications/RStudio.app/Contents/MacOS/quarto/bin` in the `.zshrc` or `.bash_profile` file on MacOS).

------------------------------------------------------------------------

All participation in the Apache Arrow project is governed by the Apache
Software Foundation’s [code of
conduct](https://www.apache.org/foundation/policies/conduct.html).
