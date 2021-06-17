# get all files
files <- list.files("./content", full.names = TRUE, pattern = "*.Rmd")
extract_r_code <- function(file, dir, index = FALSE){
  bn <- basename(file)
  fn <- strsplit(bn, ".Rmd")
  # prefix index file with "setup" so is run first
  if (index) {
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

td <- tempfile()
on.exit(unlink(td))
dir.create(td)
# Extract R code from other files
purrr::walk(files[!grepl("index.Rmd$", files)], extract_r_code, dir = td)
# extract R code from index file
extract_r_code(files[grepl("index.Rmd$", files)], td, index = TRUE)
testthat::test_dir(path = td)
