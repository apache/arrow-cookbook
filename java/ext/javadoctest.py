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

import os
import pathlib
import subprocess
from typing import Any, Dict
import tempfile
import shutil

from sphinx.ext.doctest import (
    DocTestBuilder,
    TestcodeDirective,
    TestoutputDirective,
    doctest,
    sphinx,
)
from sphinx.locale import __


class JavaTestcodeDirective(TestcodeDirective):
    def run(self):
        node_list = super().run()
        node_list[0]["language"] = "java"
        return node_list


class JavaDocTestBuilder(DocTestBuilder):
    """
    Runs java test snippets in the documentation.

    The code in each testcode block is insert into a template Maven project,
    run through exec:java, its output captured and post-processed, and finally
    compared to whatever's in the corresponding testoutput.
    """

    name = "javadoctest"
    epilog = __(
        "Java testing of doctests in the sources finished, look at the "
        "results in %(outdir)s/output.txt."
    )

    def compile(
        self, code: str, name: str, type: str, flags: Any, dont_inherit: bool
    ) -> Any:
        source_dir = pathlib.Path(__file__).parent.parent / "source" / "demo"

        with tempfile.TemporaryDirectory() as project_dir:
            shutil.copytree(source_dir, project_dir, dirs_exist_ok=True)

            template_file_path = (
                pathlib.Path(project_dir)
                / "src"
                / "main"
                / "java"
                / "org"
                / "example"
                / "Example.java"
            )

            with open(template_file_path, "r") as infile:
                template = infile.read()

            filled_template = self.fill_template(template, code)

            with open(template_file_path, "w") as outfile:
                outfile.write(filled_template)

            # Support JPMS (since Arrow 16)
            modified_env = os.environ.copy()
            modified_env["_JAVA_OPTIONS"] = "--add-opens=java.base/java.nio=ALL-UNNAMED"

            test_proc = subprocess.Popen(
                [
                    "mvn",
                    "--batch-mode",
                    "-f",
                    project_dir,
                    "compile",
                    "exec:java",
                    "-Dexec.mainClass=org.example.Example",
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
                env=modified_env,
            )

            out_java_arrow, err_java_arrow = test_proc.communicate()

        # continue with python logic code to do java output validation
        output = f"print('''{self.clean_output(out_java_arrow)}''')"

        # continue with sphinx default logic
        return compile(output, name, self.type, flags, dont_inherit)

    def clean_output(self, output: str):
        lines = output.split("\n")

        # Remove log lines from output
        lines = [l for l in lines if not l.startswith("[INFO]")]
        lines = [l for l in lines if not l.startswith("[WARNING]")]
        lines = [l for l in lines if not l.startswith("Download")]
        lines = [l for l in lines if not l.startswith("Progress")]

        result = "\n".join(lines)

        # Sometimes the testoutput content is smushed onto the same line as
        # following log line, probably just when the testcode code doesn't print
        # its own final newline. This example is the only case I found so I
        # didn't pull out the re module (i.e., this works)
        result = result.replace(
            "[INFO] ------------------------------------------------------------------------",
            "",
        )

        # Convert all tabs to 4 spaces, Sphinx seems to eat tabs even if we
        # explicitly put them in the testoutput block so we instead modify
        # the output
        result = (4 * " ").join(result.split("\t"))

        return result.strip()

    def fill_template(self, template, code):
        # Detect the special case where cookbook code is already wrapped in a
        # class and just use the code as-is without wrapping it up
        if code.find("public class Example") >= 0:
            return template + code

        # Split input code into imports and not-imports
        lines = code.split("\n")
        code_imports = [l for l in lines if l.startswith("import")]
        code_rest = [l for l in lines if not l.startswith("import")]

        pieces = [
            template,
            "\n".join(code_imports),
            "\n\npublic class Example {\n    public static void main(String[] args) {\n",
            "\n".join(code_rest),
            "    }\n}",
        ]

        return "\n".join(pieces)


def setup(app) -> Dict[str, Any]:
    app.add_directive("testcode", JavaTestcodeDirective)
    app.add_directive("testoutput", TestoutputDirective)
    app.add_builder(JavaDocTestBuilder)
    app.add_config_value("doctest_show_successes", True, False)
    # this config value adds to sys.path
    app.add_config_value("doctest_path", [], False)
    app.add_config_value("doctest_test_doctest_blocks", "default", False)
    app.add_config_value("doctest_global_setup", "", False)
    app.add_config_value("doctest_global_cleanup", "", False)
    app.add_config_value(
        "doctest_default_flags",
        doctest.DONT_ACCEPT_TRUE_FOR_1
        | doctest.ELLIPSIS
        | doctest.IGNORE_EXCEPTION_DETAIL,
        False,
    )
    return {"version": sphinx.__display_version__, "parallel_read_safe": True}
