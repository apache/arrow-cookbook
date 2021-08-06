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

TEST(BasicArrow, ReturnNotOkNoMacro) {
  StartRecipe("ReturnNotOkNoMacro");
  function<arrow::Status()> test_fn = [] {
    arrow::NullBuilder builder;
    arrow::Status st = builder.Reserve(2);
    // Tedious return value check
    if (!st.ok()) {
      return st;
    }
    st = builder.AppendNulls(-1);
    // Tedious return value check
    if (!st.ok()) {
      return st;
    }
    rout << "Appended -1 null values?" << endl;
    return arrow::Status::OK();
  };
  arrow::Status st = test_fn();
  rout << st << endl;
  EndRecipe("ReturnNotOkNoMacro");
  ASSERT_FALSE(st.ok());
}

TEST(BasicArrow, ReturnNotOk) {
  StartRecipe("ReturnNotOk");
  function<arrow::Status()> test_fn = [] {
    arrow::NullBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(2));
    ARROW_RETURN_NOT_OK(builder.AppendNulls(-1));
    rout << "Appended -1 null values?" << endl;
    return arrow::Status::OK();
  };
  arrow::Status st = test_fn();
  rout << st << endl;
  EndRecipe("ReturnNotOk");
  ASSERT_FALSE(st.ok());
}
