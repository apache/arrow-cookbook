all: html


html: py r
	@echo "\n\n>>> Cookbooks (except C++) Available in ./build <<<"


test: pytest rtest


help:
	@echo "make all         Build cookbook for all platforms in HTML, will be available in ./build"
	@echo "make test        Test cookbook for all platforms."
	@echo "make py          Build the Cookbook for Python only."
	@echo "make r           Build the Cookbook for R only."
	@echo "make pytest      Verify the cookbook for Python only."
	@echo "make rtest       Verify the cookbook for R only."


pydeps:
	@echo ">>> Installing Python Dependencies <<<\n"
	cd python && pip install -r requirements.txt


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


r: rdeps
	@echo ">>> Building R Cookbook <<<\n"
	R -s -e 'bookdown::render_book("./r/content", output_format = "bookdown::gitbook")'
	mkdir -p build/r
	cp -r r/content/_book/* build/r


rtest: rdeps
	@echo ">>> Testing R Cookbook <<<\n"
	cd ./r && Rscript ./scripts/test.R


cpptest:
	@echo ">>> Running C++ Tests/Snippets <<<\n"
	rm -rf cpp/recipe-test-build
	mkdir cpp/recipe-test-build
	cd cpp/recipe-test-build && cmake ../code -DCMAKE_BUILD_TYPE=Debug && cmake --build . && ctest --output-on-failure -j 1
	mkdir -p cpp/build
	cp cpp/recipe-test-build/recipes_out.arrow cpp/build


cpp: cpptest
	@echo ">>> Building C++ Cookbook <<<\n"
	cd cpp && make html
	mkdir -p build/cpp
	cp -r cpp/build/html/* build/cpp
