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

from pathlib import Path
import os
import pyarrow as pa
import pyarrow.ipc as ipc

from docutils.nodes import literal_block, caption, container
from sphinx.util.docutils import SphinxDirective
from sphinx.directives.code import LiteralInclude
from sphinx.util import logging

logger = logging.getLogger(__name__)
test_names_to_output = {}


class RecipeDirective(SphinxDirective):

    required_arguments = 2
    optional_arguments = 0
    option_spec = LiteralInclude.option_spec

    def run(self):
        global test_names_to_output
        filename, recipe_name = self.arguments
        if recipe_name not in test_names_to_output:
            raise Exception(
                f'Could not locate recipe output for the recipe {filename}:{recipe_name}')
        recipe_output = test_names_to_output[recipe_name]

        self.options['start-after'] = f'StartRecipe("{recipe_name}")'
        self.options['end-before'] = f'EndRecipe("{recipe_name}")'
        self.options['language'] = 'cpp'

        out_nodes = LiteralInclude.run(self)

        output_container = container('', literal_block=True, classes=[
                                     'literal-block-wrapper'])
        literal_node = literal_block(recipe_output, recipe_output)
        caption_node = caption('Code Output', 'Code Output')
        output_container += caption_node
        output_container += literal_node

        return out_nodes + [output_container]


def load_recipe_output(path):
    global test_names_to_output
    with ipc.open_stream(path) as reader:
        table = reader.read_all()
    recipe_names = table.column('Recipe Name').to_pylist()
    recipe_outputs = table.column('Recipe Output').to_pylist()
    for name, output in zip(recipe_names, recipe_outputs):
        test_names_to_output[name] = output


def locate_latest_recipe_outputs():
    base_dir = os.path.join(os.path.dirname(__file__), '..')
    matches = list(Path(base_dir).rglob('recipes_out.arrow'))
    if len(matches) == 0:
        raise Exception(
            f"Could not find any recipes_out.arrow files in {base_dir}.  Have you run the C++ tests/recipes?")

    matches_with_mtimes = [
        {'path': str(match), 'mtime': os.path.getmtime(str(match))} for match in matches]
    sorted_matches = [match['path'] for match in sorted(
        matches_with_mtimes, key=lambda x: x['mtime'])]
    for match_with_mtime in matches_with_mtimes:
        path = match_with_mtime['path']
        mtime = match_with_mtime['mtime']
        logger.info(f'Considering recipe file {path} last updated at {mtime}')
    return sorted_matches[-1]


def setup(app):

    recipe_outputs_path = locate_latest_recipe_outputs()
    logger.info(f'Located recipes file: {recipe_outputs_path}')

    load_recipe_output(recipe_outputs_path)

    app.add_directive('recipe', RecipeDirective)

    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
