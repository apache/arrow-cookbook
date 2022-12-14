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

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <new_version>"
  exit 1
fi

new_version=$1
version_tag="apache-arrow-${new_version}"
current_branch=$(git branch --show-current)

if [ ${current_branch} != "main" ]; then
    echo "You should create a tag and update stable from the main branch instead of ${current_branch}:"
    echo "1. Checkout the default branch and pull the latest changes."
    echo "Commands:"
    echo "   git checkout main"
    echo "   git pull"
    echo "   dev/release/02-update.sh ${new_version}"
    exit 1
fi

if [ $(git branch -l "stable") ]; then
    echo "Removing old stable branch locally and remotely"
    git branch -d stable
    git push -u apache --delete stable
fi

echo "Creating and pushing new stable branch from main"
git branch stable
git push -u apache stable

if [ $(git tag -l "${version_tag}") ]; then
  echo "Delete existing git tag $version_tag"
  git tag -d "${version_tag}"
fi

echo "Creating tag ${version_tag} for ${new_version}"
git tag -a "${version_tag}" -m "[Release] Apache Arrow Release ${new_version}"
git push -u apache ${version_tag}
