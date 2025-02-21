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

# Apache Arrow Cookbook Release update

The following explains how to update the Cookbooks once a new Release of Apache
Arrow has been created. Note that updating the cookbooks first requires updated
packages to be available on either conda-forge (C++, Python) or Maven Central
(Java).

## Requirements

For the C++ cookbooks we use conda lock files that have to be updated when we
want to update the version of Arrow used.

The script requires `conda-lock` to be installed. As an example you can create a
virtual environment with the following commands but you can use conda too. The
only requirement is for `conda-lock` to be available.

```sh
python -m venv cookbook-release
source cookbook-release/bin/activate
pip install conda-lock
```

## Usage

By default, the following script only updates the C++ and Python cookbook
versions because Arrow Java is released and versioned separately.

To update versions for C++ and Python, execute the `01-bump-versions.sh` script
with two arguments `current_version` and `new_version`:

```sh
./dev/release/01-bump-versions.sh 10.0.1 11.0.0
```

The script will:

- Update the version for Python and CPP cookbooks.
- Update the conda lock files for the CPP cookbooks.
- Commit to the current branch with the updated versions.

To update just versions for Arrow Java, run:

```sh
BUMP_DEFAULT=0 BUMP_JAVA=1 ./dev/release/01-bump-versions.sh 10.0.1 11.0.0
```

When run this way, this script will:

The script will:

- Update the version in the `pom.xml` used by the Java Cookbooks.
- Commit to the current branch with the updated versions.

Either way, after running you should create a Pull Request to merge the changes
against the main branch.

Once the Pull Request is merged you can run the `02-update.sh` from the updated
main branch. This script requires a single argument with the `new_version`:

```sh
./dev/release/02-update.sh 11.0.0
```

The script will:

- Regenerate the stable branch from main. Take into account that this will
  delete and create a new stable branch.
- Create a tag for the stable Release.
- Push both the new stable branch and the tag.
