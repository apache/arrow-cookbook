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
#include <arrow/visit_array_inline.h>
#include <gtest/gtest.h>

#include "common.h"

arrow::Status ReturnNotOkMacro() {
  StartRecipe("ReturnNotOkNoMacro");
  std::function<arrow::Status()> test_fn = [] {
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
    rout << "Appended -1 null values?" << std::endl;
    return arrow::Status::OK();
  };
  arrow::Status st = test_fn();
  rout << st << std::endl;
  EndRecipe("ReturnNotOkNoMacro");
  EXPECT_FALSE(st.ok());
  return arrow::Status::OK();
}

arrow::Status ReturnNotOk() {
  StartRecipe("ReturnNotOk");
  std::function<arrow::Status()> test_fn = [] {
    arrow::NullBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(2));
    ARROW_RETURN_NOT_OK(builder.AppendNulls(-1));
    rout << "Appended -1 null values?" << std::endl;
    return arrow::Status::OK();
  };
  arrow::Status st = test_fn();
  rout << st << std::endl;
  EndRecipe("ReturnNotOk");
  EXPECT_FALSE(st.ok());
  return arrow::Status::OK();
}

TEST(BasicArrow, ReturnNotOkNoMacro) { ASSERT_OK(ReturnNotOkMacro()); }

TEST(BasicArrow, ReturnNotOk) { ASSERT_OK(ReturnNotOk()); }

/// \brief Sum numeric values across columns
///
/// Only supports floating point and integral types. Does not support decimals.
class TableSummation {
  double partial = 0.0;
 public:

  arrow::Result<double> Compute(std::shared_ptr<arrow::RecordBatch> batch) {
    for (std::shared_ptr<arrow::Array> array : batch->columns()) {
      ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*array, this));
    }
    return partial;
  }

  // Default implementation
  arrow::Status Visit(const arrow::Array& array) {
    return arrow::Status::NotImplemented("Can not compute sum for array of type ",
                                         array.type()->ToString());
  }

  template <typename ArrayType, typename T = typename ArrayType::TypeClass>
  arrow::enable_if_number<T, arrow::Status> Visit(const ArrayType& array) {
    for (arrow::util::optional<typename T::c_type> value : array) {
      if (value.has_value()) {
        partial += static_cast<double>(value.value());
      }
    }
    return arrow::Status::OK();
  }
};  // TableSummation

arrow::Status VisitorSummationExample() {
  StartRecipe("VisitorSummationExample");
  std::shared_ptr<arrow::Schema> schema = arrow::schema({
      arrow::field("a", arrow::int32()),
      arrow::field("b", arrow::float64()),
  });
  int32_t num_rows = 3;
  std::vector<std::shared_ptr<arrow::Array>> columns;

  arrow::Int32Builder a_builder = arrow::Int32Builder();
  std::vector<int32_t> a_vals = {1, 2, 3};
  ARROW_RETURN_NOT_OK(a_builder.AppendValues(a_vals));
  ARROW_ASSIGN_OR_RAISE(auto a_arr, a_builder.Finish());
  columns.push_back(a_arr);

  arrow::DoubleBuilder b_builder = arrow::DoubleBuilder();
  std::vector<double> b_vals = {4.0, 5.0, 6.0};
  ARROW_RETURN_NOT_OK(b_builder.AppendValues(b_vals));
  ARROW_ASSIGN_OR_RAISE(auto b_arr, b_builder.Finish());
  columns.push_back(b_arr);

  auto batch = arrow::RecordBatch::Make(schema, num_rows, columns);

  // Call
  TableSummation summation;
  ARROW_ASSIGN_OR_RAISE(auto total, summation.Compute(batch));

  rout << "Total is " << total;

  EndRecipe("VisitorSummationExample");

  EXPECT_EQ(total, 21.0);
  return arrow::Status::OK();
}

TEST(BasicArrow, VisitorSummationExample) { ASSERT_OK(VisitorSummationExample()); }
