# Building the R Cookbook

The R cookbook is based on `bookdown`.

Running ``make r`` from the cookbook root directory (the one where the ``Makefile`` exists) will install all necessary dependencies (including the latest nightly build of the Arrow R package) and will compile the cookbook to HTML.

You will see the compiled result inside the ``build/r`` directory.

# Testing R Recipes

All recipes in the cookbook must be tested. The cookbook uses `testthat` to verify the recipes.

Running ``make rtest`` from the cookbook root directory will verify that the code for all the recipes runs correctly and provides the expected output.

# Adding R Recipes

The recipes are written in RMarkdown format using `bookdown`.

New recipes can be added to one of the existing ``.Rmd`` files if they suit that section or you can create new sections by adding additional ``.Rmd`` files in the `content` directory. You just need to remember to add them to the `_bookdown.yml` file in the `rmd_files` for them to become visible.

Each code block in the recipe must be followed by an accompanying test block.  Each major code chunk must be given a descriptive label, and be immediately followed by a test of its output.  This test's label should be labelled "test_" followed by the name of the chunk that it's testing.  It should also have the `opts.label` attribute set to `"test"` - this will ensure that the test is not rendered as part of the cookbook.

Here's an example:

~~~
```{r, write_parquet}
# Create table
my_table <- Table$create(tibble::tibble(group = c("A", "B", "C"), score = c(99, 97, 99)))

# Write to Parquet
write_parquet(my_table, "my_table.parquet")
```

```{r, test_write_parquet, opts.label = "test"}
test_that("write_parquet chunk works as expected", {
  expect_true(file.exists("my_table.parquet"))
})
```
~~~
