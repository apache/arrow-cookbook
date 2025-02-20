#!/usr/bin/env bash
#
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
#
set -ue

: ${BUMP_DEFAULT:=1}
: ${BUMP_CPP:=${BUMP_DEFAULT}}
: ${BUMP_PYTHON:=${BUMP_DEFAULT}}
: ${BUMP_JAVA:=0}

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <old_version> <new_version>"
  exit 1
fi

. $SOURCE_DIR/utils-prepare.sh

old_version=$1
new_version=$2
version_tag="apache-arrow-${new_version}"

# Use the BUMP_XYZ vars to create a tag string for the commit
TAGS=""
if [ "${BUMP_CPP:-0}" -eq 1 ]; then
  TAGS="${TAGS}[C++]"
fi
if [ "${BUMP_JAVA:-0}" -eq 1 ]; then
  TAGS="${TAGS}[Java]"
fi
if [ "${BUMP_PYTHON:-0}" -eq 1 ]; then
  TAGS="${TAGS}[Python]"
fi

echo "Prepare ${new_version}"
update_versions "${old_version}" "${new_version}"
git commit -m "MINOR: [Release]$TAGS Update versions for ${new_version}"
