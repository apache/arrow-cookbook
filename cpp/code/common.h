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

#ifndef ARROW_COOKBOOK_COMMON_H
#define ARROW_COOKBOOK_COMMON_H

#include <arrow/testing/gtest_util.h>

#include <sstream>
#include <string>

inline std::stringstream rout;

void StartRecipe(const std::string& recipe_name);
void EndRecipe(const std::string& recipe_name);
arrow::Status DumpRecipeOutput(const std::string& output_filename);
bool HasRecipeOutput();
arrow::Result<std::string> FindTestDataFile(const std::string& test_data_name);

#endif  // ARROW_COOKBOOK_COMMON_H
