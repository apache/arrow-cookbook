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
  if (bn == "index") {
    prefix <- "setup"
  } else {
    prefix <- "test"
  }

  outpath <- file.path(dir, paste0(prefix, "-", fn, ".R"))
  knitr::purl(
    input = file,
    output = outpath,
    # If we output text chunks as comments, the line number where the error was should match with the one reported in the tempfile; needs more testing
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
