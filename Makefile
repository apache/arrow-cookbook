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
all: html


html: py r java
	@echo "\n\n>>> Cookbooks (except C++) Available in ./build <<<"


test: pytest rtest javatest


help:
	@echo "make all         Build cookbook for all platforms in HTML, will be available in ./build"
	@echo "make test        Test cookbook for all platforms."
	@echo "make py          Build the Cookbook for Python only."
	@echo "make r           Build the Cookbook for R only."
	@echo "make pytest      Verify the cookbook for Python only."
	@echo "make rtest       Verify the cookbook for R only."
	@echo "make java        Build the Cookbook for Java only."
	@echo "make javatest    Verify the cookbook for Java only."

pydeps:
	@echo ">>> Installing Python Dependencies <<<\n"
	cd python && pip install -r requirements.txt
ifeq ($(ARROW_NIGHTLY), 1)
	pip install --upgrade --extra-index-url https://pypi.fury.io/arrow-nightlies/ --prefer-binary --pre pyarrow
endif


javadeps:
	@echo ">>> Installing Java Dependencies <<<\n"
	cd java && pip install -r requirements.txt

py: pydeps
	@echo ">>> Building Python Cookbook <<<\n"
	cd python && make html
	mkdir -p build/py
	cp -r python/build/html/* build/py


pytest: pydeps
	@echo ">>> Testing Python Cookbook <<<\n"
	cd python && make doctest

rdeps:
	@echo ">>> Installing R Dependencies <<<\n"
ifdef arrow_r_version
	cd ./r && Rscript ./scripts/install_dependencies.R $(arrow_r_version)
else
	cd ./r && Rscript ./scripts/install_dependencies.R
endif
ifeq ($(ARROW_NIGHTLY), 1)
	pip install --upgrade --extra-index-url https://pypi.fury.io/arrow-nightlies/ --prefer-binary --pre pyarrow
else
	pip install pyarrow
endif


r: rdeps
	@echo ">>> Building R Cookbook <<<\n"
	R -s -e 'bookdown::render_book("./r/content", output_format = "bookdown::gitbook")'
	mkdir -p build/r
	cp -r r/content/_book/* build/r


rtest: rdeps
	@echo ">>> Testing R Cookbook <<<\n"
	cd ./r && Rscript ./scripts/test.R


# Only release mode is supported because of a Protobuf WONTFIX bug
# https://github.com/protocolbuffers/protobuf/issues/9947
cpptest:
	@echo ">>> Running C++ Tests/Snippets <<<\n"
	rm -rf cpp/recipe-test-build
	mkdir cpp/recipe-test-build
	cd cpp/recipe-test-build && cmake ../code -DCMAKE_BUILD_TYPE=Release && cmake --build . && ctest --output-on-failure -j 1
	mkdir -p cpp/build
	cp cpp/recipe-test-build/recipes_out.arrow cpp/build


cpp: cpptest
	@echo ">>> Building C++ Cookbook <<<\n"
	cd cpp && make html
	mkdir -p build/cpp
	cp -r cpp/build/html/* build/cpp


.PHONY: java
java: javadeps
	@echo ">>> Building Java Cookbook <<<\n"
	cd java && make html
	mkdir -p build/java
	cp -r java/build/html/* build/java


javatest: javadeps
	@echo ">>> Testing Java Cookbook <<<\n"
	cd java && make javadoctest
