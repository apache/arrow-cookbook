# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Extract R code from .Rmd files
#'
#' Extracts all the R code and tests from .Rmd files and puts them in separate files
#' within directory `dir`.  In order to preserve line numbering in testthat tests,
#' all markdown chunks are also extracted as comments.
#'
#' @param file Path to .Rmd file
#' @param dir Directory in which to put extracted R files
extract_r_code <- function(file, dir){
  bn <- basename(file)
  fn <- strsplit(bn, ".Rmd")
  # prefix index file with "setup" so is run first as it may contain dependencies
  if (startsWith(bn, "index")) {
    prefix <- "setup"
  } else {
    prefix <- "test"
  }

  outpath <- file.path(dir, paste0(prefix, "-", fn, ".R"))
  knitr::purl(
    input = file,
    output = outpath,
    # If we output text chunks as comments, the line number where the error is
    # reported in the tests should match with the correct line in the Rmd
    documentation = 2L,
    quiet = TRUE
  )

}

# get all files
files <- list.files("./content", full.names = TRUE, pattern = "*.Rmd")

# set up a temporary directory to work with
td <- tempfile()
on.exit(unlink(td))
dir.create(td)

# Extract R code from files
purrr::walk(files, extract_r_code, dir = td)

# Run tests
testthat::test_dir(path = td)
