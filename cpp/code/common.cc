// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "common.h"

#include <filesystem>
#include <sstream>
#include <unordered_map>

#include "arrow/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"

static arrow::StringBuilder test_names_builder;
static arrow::StringBuilder test_output_builder;
static std::string current_recipe;

void StartRecipe(const std::string& recipe_name) {
  if (!current_recipe.empty()) {
    FAIL() << "Attempt to start a recipe " << recipe_name << " but the recipe "
           << current_recipe << " has not been marked finished";
  }
  if (recipe_name.empty()) {
    FAIL() << "Invalid empty recipe name";
  }
  current_recipe = recipe_name;
  rout = std::stringstream();
}

void EndRecipe(const std::string& recipe_name) {
  if (current_recipe != recipe_name) {
    FAIL() << "Attempt to end a recipe " << recipe_name
           << " but the recipe was not in progress";
  }
  std::string recipe_output = rout.str();

  ASSERT_OK(test_names_builder.Append(recipe_name));
  ASSERT_OK(test_output_builder.Append(recipe_output));

  current_recipe = "";
}

std::shared_ptr<arrow::Schema> RecipesTableSchema() {
  return arrow::schema({arrow::field("Recipe Name", arrow::utf8()),
                        arrow::field("Recipe Output", arrow::utf8())});
}

std::shared_ptr<arrow::Array> MakeEmptyStringArray() {
  arrow::StringBuilder builder;
  return builder.Finish().ValueOrDie();
}

arrow::Result<std::shared_ptr<arrow::Table>> MakeEmptyRecipesTable() {
  std::shared_ptr<arrow::Schema> schema = RecipesTableSchema();
  std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(
      schema, 0, {MakeEmptyStringArray(), MakeEmptyStringArray()});
  return arrow::Table::FromRecordBatches({batch});
}

arrow::Result<std::shared_ptr<arrow::Table>> ReadRecipeTable(
    const std::shared_ptr<arrow::io::RandomAccessFile>& in_file) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader,
                        arrow::ipc::RecordBatchStreamReader::Open(in_file));
  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadAll(&table));
  return table;
}

arrow::Result<std::shared_ptr<arrow::Table>> LoadExistingRecipeOutputTable(
    const std::string& filename) {
  std::shared_ptr<arrow::fs::FileSystem> fs =
      std::make_shared<arrow::fs::LocalFileSystem>();
  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> maybe_in =
      fs->OpenInputFile(filename);
  if (!maybe_in.ok()) {
    return MakeEmptyRecipesTable();
  }
  std::shared_ptr<arrow::io::RandomAccessFile> in = *maybe_in;
  return ReadRecipeTable(in);
}

arrow::Result<std::shared_ptr<arrow::Table>> CreateRecipeOutputTable() {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> test_names,
                        test_names_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> test_outputs,
                        test_output_builder.Finish());
  std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field("Recipe Name", arrow::utf8()),
                     arrow::field("Recipe Output", arrow::utf8())});
  std::shared_ptr<arrow::RecordBatch> batch =
      arrow::RecordBatch::Make(schema, test_names->length(), {test_names, test_outputs});
  return arrow::Table::FromRecordBatches({batch});
}

void PopulateMap(const arrow::Table& table,
                 std::unordered_map<std::string, std::string>* values) {
  if (table.num_rows() == 0) {
    return;
  }
  std::shared_ptr<arrow::StringArray> table_names =
      std::dynamic_pointer_cast<arrow::StringArray>(table.column(0)->chunk(0));
  std::shared_ptr<arrow::StringArray> table_outputs =
      std::dynamic_pointer_cast<arrow::StringArray>(table.column(1)->chunk(0));
  for (int64_t i = 0; i < table.num_rows(); i++) {
    values->insert({table_names->GetString(i), table_outputs->GetString(i)});
  }
}

arrow::Result<std::shared_ptr<arrow::Table>> MergeRecipeTables(
    const std::shared_ptr<arrow::Table>& old_table,
    const std::shared_ptr<arrow::Table>& new_table) {
  std::unordered_map<std::string, std::string> values;
  PopulateMap(*old_table, &values);
  PopulateMap(*new_table, &values);
  arrow::StringBuilder names_builder;
  arrow::StringBuilder outputs_builder;
  for (const auto& pair : values) {
    ARROW_RETURN_NOT_OK(names_builder.Append(pair.first));
    ARROW_RETURN_NOT_OK(outputs_builder.Append(pair.second));
  }
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> names_arr, names_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> outputs_arr,
                        outputs_builder.Finish());
  std::shared_ptr<arrow::Schema> schema = RecipesTableSchema();
  std::shared_ptr<arrow::RecordBatch> batch =
      arrow::RecordBatch::Make(schema, names_arr->length(), {names_arr, outputs_arr});
  return arrow::Table::FromRecordBatches({batch});
}

bool HasRecipeOutput() { return test_names_builder.length() > 0; }

arrow::Status DumpRecipeOutput(const std::string& output_filename) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> new_table,
                        CreateRecipeOutputTable());
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> old_table,
                        LoadExistingRecipeOutputTable(output_filename));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> merged_table,
                        MergeRecipeTables(old_table, new_table));
  std::shared_ptr<arrow::fs::FileSystem> fs =
      std::make_shared<arrow::fs::LocalFileSystem>();
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::io::OutputStream> out_stream,
                        fs->OpenOutputStream(output_filename));
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<arrow::ipc::RecordBatchWriter> writer,
      arrow::ipc::MakeStreamWriter(out_stream.get(), merged_table->schema()));
  RETURN_NOT_OK(writer->WriteTable(*merged_table));
  return writer->Close();
}

arrow::Result<std::string> FindTestDataFile(const std::string& test_data_name) {
  auto path_iter = std::filesystem::current_path();
  while (path_iter.has_parent_path()) {
    auto test_data_dir_path = path_iter / "testdata";
    if (std::filesystem::exists(test_data_dir_path)) {
      return (test_data_dir_path / test_data_name).string();
    }
    path_iter = path_iter.parent_path();
  }
  return arrow::Status::Invalid(
      "Could not locate testdata directory.  Tests must be "
      "run inside of the cookbook repo");
}