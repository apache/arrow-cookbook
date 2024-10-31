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

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
import os
import sys

sys.path.append(os.path.abspath("../ext"))


# -- Project information -----------------------------------------------------

project = 'Apache Arrow Java Cookbook'
copyright = '2022, Apache Software Foundation'
author = 'The Apache Software Foundation'
arrow_nightly=os.getenv("ARROW_NIGHTLY")
if arrow_nightly and arrow_nightly != '0':
    version = "18.0.0-SNAPSHOT"
else:
    version = "17.0.0"
print(f"Running with Arrow version: {version}")

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "javadoctest",
    "sphinx.ext.intersphinx"
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'alabaster'

html_theme_options = {
    'page_width': '1200px',
    'code_font_size': '0.8em',
    "logo": "arrow-logo_vertical_black-txt_transparent-bg.svg",
    "github_user": "apache",
    "github_repo": "arrow-cookbook",
    "github_button": "true",
    "github_type": "none",
    "extra_nav_links": {
        "User Guide": "https://arrow.apache.org/docs/java/index.html",
        "API Reference": "https://arrow.apache.org/docs/java/reference/index.html",
        "All Cookbooks": "../"
    },
    "font_family": "-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,Liberation Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji"
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['../../static']

# The name of an image file (relative to this directory) to use as a favicon of
# the docs.  This file should be a Windows icon file (.ico) being 16x16 or
# 32x32 pixels large.
#
html_favicon = "../../static/favicon.ico"
