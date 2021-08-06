# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

args <- commandArgs(trailingOnly = TRUE)

install_release <- FALSE
build_version <- NA

# get arguments used to run this script
if (length(args) == 0) {
  install_release <- TRUE
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
load_package <- function(pkg_name){
  if (!require(pkg_name, character.only = TRUE)) {
    install.packages(pkg_name)
  } 
  library(pkg_name, character.only = TRUE)
}

dependencies <- c("testthat", "bookdown", "knitr", "purrr", "remotes", "dplyr")

lapply(dependencies, load_package)



#' Install the appropriate Arrow version
#' 
#' If `build_release` is TRUE, install the latest version of Arrow, else 
#' installs the specified version 
#' 
#' @param release_version Install last released Arrow R package? Logical.
#' @param build_version The version to install
install_arrow_version <- function(release_version = FALSE, build_version = NULL){
  
  installed_version <- get_installed_version("arrow")
  
  if (release_version) {
    
    last_release <- available.packages()["arrow",]["Version"]
    
    # Only install the latest released version if it's not already installed
    if (package_version(last_release) != installed_version) {
      Sys.setenv(NOT_CRAN = TRUE)
      install.packages("arrow")
    }
  } else {
    # Otherwise installed the build version specified if not already installed
    if (installed_version != build_version) {
      remotes::install_version("arrow", version = build_version)
    }
  }
}

install_arrow_version(install_release, build_version)
