from sphinx.ext.doctest import *
from sphinx.locale import __
import subprocess
from subprocess import Popen,PIPE
import os

class JavaTestDirective(TestDirective):
    pass

class JavaDocTestBuilder(DocTestBuilder):
    """
    Runs java test snippets in the documentation.
    """
    name = 'javadoctest'
    epilog = __('Java testing of doctests in the sources finished, look at the '
                'results in %(outdir)s/output.txt.')
    def compile(self, code: str, name: str, type: str, flags: Any, dont_inherit: bool) -> Any:
        # go to project that contains all your arrow maven dependencies
        cwd = os.getcwd()
        os.chdir('./source/demo')

        # create list of all arrow jar dependencies
        proc_mvn_dependency = subprocess.Popen(['mvn', '-q', 'dependency:build-classpath', '-DincludeTypes=jar', '-Dmdep.outputFile=.cp.tmp'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout_mvn, stderr_mvn = proc_mvn_dependency.communicate()
        if not os.path.exists('.cp.tmp'):
            raise RuntimeError(__('invalid process to create jshell dependencies library'))

        # get list of all arrow jar dependencies
        proc_get_dependency = subprocess.Popen(['cat', '.cp.tmp'], stdout=subprocess.PIPE, text=True)
        stdout_dependency, stderr_dependency = proc_get_dependency.communicate()
        if not stdout_dependency:
            raise RuntimeError(__('invalid process to list jshell dependencies library'))

        # execute java testing code thru jshell and read output
        proc_java_arrow_code = subprocess.Popen(['echo', '' + code], stdout=subprocess.PIPE)
        proc_jshell_process = subprocess.Popen(['jshell', '--class-path', stdout_dependency, '-'], stdin=proc_java_arrow_code.stdout, stdout=subprocess.PIPE, text=True)
        proc_java_arrow_code.stdout.close()
        stdout_java_arrow, stderr_java_arrow = proc_jshell_process.communicate()
        if stderr_java_arrow:
            raise RuntimeError(__('invalid process to run jshell por arrow project'))

        # continue with python logic code to do java output validation battle tested
        output = 'print('+stdout_java_arrow+')'

        # go to default directory 
        os.chdir(cwd)

        # continue with sphinx default logic
        return compile(output, name, self.type, flags, dont_inherit)

def setup(app: "ArrowSphinx") -> Dict[str, Any]:
    app.add_directive('testcode', TestcodeDirective)
    app.add_directive('testoutput', TestoutputDirective)
    app.add_builder(JavaDocTestBuilder)
    # this config value adds to sys.path
    app.add_config_value('doctest_path', [], False)
    app.add_config_value('doctest_test_doctest_blocks', 'default', False)
    app.add_config_value('doctest_global_setup', '', False)
    app.add_config_value('doctest_global_cleanup', '', False)
    app.add_config_value(
        'doctest_default_flags',
        doctest.DONT_ACCEPT_TRUE_FOR_1 | doctest.ELLIPSIS | doctest.IGNORE_EXCEPTION_DETAIL,
        False)
    return {'version': sphinx.__display_version__, 'parallel_read_safe': True}