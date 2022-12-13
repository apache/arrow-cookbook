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

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <old_version> <new_version>"
  exit 1
fi

. $SOURCE_DIR/utils-prepare.sh

old_version=$1
new_version=$2
version_tag="apache-arrow-${new_version}"

: ${PREPARE_DEFAULT:=1}
: ${PREPARE_VERSION_PRE_TAG:=${PREPARE_DEFAULT}}
: ${PREPARE_TAG:=${PREPARE_DEFAULT}}


if [ ${PREPARE_TAG} -gt 0 ]; then
  if [ $(git tag -l "${version_tag}") ]; then
    echo "Delete existing git tag $version_tag"
    git tag -d "${version_tag}"
  fi
fi

if [ ${PREPARE_VERSION_PRE_TAG} -gt 0 ]; then
  echo "Prepare ${new_version} on tag ${version_tag}"
  update_versions "${old_version}" "${new_version}"
  git commit -m "MINOR: [Release] Update versions for ${new_version}"
fi

if [ ${PREPARE_TAG} -gt 0 ]; then
  git tag -a "${version_tag}" -m "[Release] Apache Arrow Release ${new_version}"
fi
