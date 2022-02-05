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

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "common.h"

TEST(CreatingArrowObjects, CreateArrays) {
  StartRecipeCollection([] {
    StartRecipe("CreatingArrays");
    arrow::Int32Builder builder;
    ARROW_RETURN_NOT_OK(builder.Append(1));
    ARROW_RETURN_NOT_OK(builder.Append(2));
    ARROW_RETURN_NOT_OK(builder.Append(3));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> arr, builder.Finish())
    rout << arr->ToString() << std::endl;
    return EndRecipe("CreatingArrays");
  });

  StartRecipeCollection([] {
    StartRecipe("CreatingArraysPtr");
    // Raw pointers
    arrow::Int64Builder long_builder = arrow::Int64Builder();
    std::array<int64_t, 4> values = {1, 2, 3, 4};
    ARROW_RETURN_NOT_OK(long_builder.AppendValues(values.data(), values.size()));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> arr, long_builder.Finish());
    rout << arr->ToString() << std::endl;

    // Vectors
    arrow::StringBuilder str_builder = arrow::StringBuilder();
    std::vector<std::string> strvals = {"x", "y", "z"};
    ARROW_RETURN_NOT_OK(str_builder.AppendValues(strvals));
    ARROW_ASSIGN_OR_RAISE(arr, str_builder.Finish());
    rout << arr->ToString() << std::endl;

    // Iterators
    arrow::DoubleBuilder dbl_builder = arrow::DoubleBuilder();
    std::set<double> dblvals = {1.1, 1.1, 2.3};
    ARROW_RETURN_NOT_OK(dbl_builder.AppendValues(dblvals.begin(), dblvals.end()));
    ARROW_ASSIGN_OR_RAISE(arr, dbl_builder.Finish());
    rout << arr->ToString() << std::endl;
    return EndRecipe("CreatingArraysPtr");
  });
}
