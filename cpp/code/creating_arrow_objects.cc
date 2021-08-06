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

using namespace std;

TEST(CreatingArrowObjects, CreateArrays) {
  StartRecipe("CreatingArrays");
  arrow::Int32Builder builder;
  ASSERT_OK(builder.Append(1));
  ASSERT_OK(builder.Append(2));
  ASSERT_OK(builder.Append(3));
  ASSERT_OK_AND_ASSIGN(shared_ptr<arrow::Array> arr, builder.Finish())
  rout << arr->ToString() << endl;
  EndRecipe("CreatingArrays");

  StartRecipe("CreatingArraysPtr");
  // Raw pointers
  arrow::Int64Builder long_builder = arrow::Int64Builder();
  array<int64_t, 4> values = {1, 2, 3, 4};
  ASSERT_OK(long_builder.AppendValues(values.data(), values.size()));
  ASSERT_OK_AND_ASSIGN(arr, long_builder.Finish());
  rout << arr->ToString() << endl;

  // Vectors
  arrow::StringBuilder str_builder = arrow::StringBuilder();
  vector<string> strvals = {"x", "y", "z"};
  ASSERT_OK(str_builder.AppendValues(strvals));
  ASSERT_OK_AND_ASSIGN(arr, str_builder.Finish());
  rout << arr->ToString() << endl;

  // Iterators
  arrow::DoubleBuilder dbl_builder = arrow::DoubleBuilder();
  set<double> dblvals = {1.1, 1.1, 2.3};
  ASSERT_OK(dbl_builder.AppendValues(dblvals.begin(), dblvals.end()));
  ASSERT_OK_AND_ASSIGN(arr, dbl_builder.Finish());
  rout << arr->ToString() << endl;
  EndRecipe("CreatingArraysPtr");
}
