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

# Set the version of Arrow to build based on command line arguments
args <- commandArgs(trailingOnly = TRUE)

if (length(args) > 0) {
  build_version <- package_version(args[1])
} else {
  build_version <- package_version(available.packages()["arrow", ]["Version"])
}

#' Get installed version of a package
#'
#' @param pkg Package name, character
#' @return Package version number, or 0.0.0 if not installed.
get_installed_version <- function(pkg) {
  tryCatch(
    packageVersion(pkg),
    error = function(e) {
      return(
        structure(
          list(
            c(0L, 0L, 0L)
          ),
          class = c("package_version", "numeric_version")
        )
      )
    }
  )
}

#' Load package, installing it first if not already installed
#'
#' @param pkg Package name, character.
load_package <- function(pkg) {
  if (!suppressWarnings(suppressMessages(require(pkg, character.only = TRUE)))) {
    install.packages(pkg)
  }
  library(pkg, character.only = TRUE)
}

#' Install a specific version of the Arrow R package
#'
#' @param version_to_install The version to install. Default is latest CRAN version.
install_arrow_version <- function(version_to_install) {

  # TODO: refactor this to get the latest available version on the nightlies
  # given we set NOT_CRAN = TRUE (#29)
  latest_release <- package_version(available.packages()["arrow", ]["Version"])
  installed_version <- get_installed_version("arrow")

  # Only install the latest released version if it's not already installed
  if (version_to_install == latest_release && installed_version != latest_release) {
    Sys.setenv(NOT_CRAN = TRUE)
    install.packages("arrow")
    # Otherwise install the build version specified if not already installed
    # TODO: refactor this to install the specific version from the nightlies if
    # a binary is available (#29)
  } else if (installed_version != version_to_install) {
    remotes::install_version("arrow", version = version_to_install)
  }
}

dependencies <- c("testthat", "bookdown", "knitr", "purrr", "remotes", "dplyr")

for (dependency in dependencies) {
  load_package(dependency)
}

install_arrow_version(build_version)
