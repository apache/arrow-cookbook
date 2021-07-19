args <- commandArgs(trailingOnly = TRUE)

# get arguments used to run this script
if (length(args) == 0) {
  build_version = "latest"
} else {
  build_version <- package_version(args[1])
}

# get installed version of a package
get_installed_version <- function(pkg){
  tryCatch(
    packageVersion(pkg),
    error = function(e) {
      return(structure(list(c(0L, 0L, 0L)), class = c("package_version", "numeric_version")))
    }
  )
}

# install dependencies if not installed
if (!require("pacman")) install.packages("pacman")
pacman::p_load("testthat", "bookdown", "xfun", "knitr", "purrr", "remotes", "dplyr")
pacman::p_load_gh("rmflight/testrmd")

# check version of Arrow installed, and install correct one
if (!inherits(build_version, "package_version") && build_version == "latest") {
  install.packages("arrow", repos = c("https://arrow-r-nightly.s3.amazonaws.com", getOption("repos")))
} else {
  installed_version <- get_installed_version("arrow")
  if (installed_version != build_version) {
    pkg_url <- paste0("https://cran.r-project.org/src/contrib/Archive/arrow/arrow_", build_version, ".tar.gz")
    install.packages(pkg_url, repos = NULL, type = "source")
  }
}
