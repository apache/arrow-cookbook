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

ARROW_COOKBOOK_DIR="${SOURCE_DIR}/../.."

update_versions() {
  local base_version=$1
  local next_version=$2

  local major_version=${next_version%%.*}
  local nightly_major_version=$(($major_version+1))
  local nightly_version_snapshot="${nightly_major_version}.0.0-SNAPSHOT"

  pushd "${ARROW_COOKBOOK_DIR}/java/source/demo"
  mvn versions:set-property -Dproperty=arrow.version -DnewVersion=${next_version}
  find . -type f -name pom.xml.versionsBackup -delete
  git add pom.xml
  popd

  pushd "${ARROW_COOKBOOK_DIR}/java/source"
  sed -i.bak -E \
    -e "s/version = \"${base_version}\"/version = \"${next_version}\"/" \
    -e "s/version = \"${next_version}-SNAPSHOT\"/version = \"${nightly_version_snapshot}\"/" \
    conf.py
  rm -f conf.py.bak
  git add conf.py
  popd

  pushd "${ARROW_COOKBOOK_DIR}/python"
  sed -i.bak -E -e \
    "s/pyarrow==${base_version}/pyarrow==${next_version}/" \
    requirements.txt
  rm -f requirements.txt.bak
  git add requirements.txt
  popd

  pushd "${ARROW_COOKBOOK_DIR}/cpp"
  sed -i.bak -E \
    -e "s/libarrow==${base_version}/libarrow==${next_version}/" \
    -e "s/pyarrow==${base_version}/pyarrow==${next_version}/" \
    environment.yml
  rm -f environment.yml.bak

  conda-lock --file environment.yml --kind explicit -p linux-aarch64 -p linux-64 -p osx-arm64
  git add environment.yml
  git add conda-linux-64.lock
  git add conda-linux-aarch64.lock
  git add conda-osx-arm64.lock
  popd
}
