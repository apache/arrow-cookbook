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

#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/visitor.h"
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

class TypeCountVisitor : public arrow::TypeVisitor {
 public:
  uint64_t nested_count;
  uint64_t non_nested_count;

  template <typename T>
  arrow::enable_if_not_nested<T, arrow::Status> Visit(const T&) {
    non_nested_count++;
    return arrow::Status::OK();
  }

  template <typename T>
  arrow::enable_if_list_type<T, arrow::Status> Visit(const T& type) {
    nested_count++;
    ARROW_RETURN_NOT_OK(type.value_type()->Accept(this));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StructType& type) {
    nested_count++;
    for (auto field : type.fields()) {
      ARROW_RETURN_NOT_OK(field->type()->Accept(this));
    }
    return arrow::Status::OK();
  }
};  // TypeCountVisitor

arrow::Status CountTypes() {
  StartRecipe("TypeVisitorSimple");
  // Create a schema
  std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field("a", arrow::int8())});

  TypeCountVisitor counter;

  for (auto field : schema->fields()) {
    ARROW_RETURN_NOT_OK(field->type()->Accept(&counter));
  }

  rout << "Found " << counter.nested_count << " nested types and "
       << counter.non_nested_count << " non-nested types.";

  EndRecipe("TypeVisitorSimple");

  EXPECT_EQ(counter.nested_count, 3);
  EXPECT_EQ(counter.non_nested_count, 6);
  return arrow::Status::OK();
}

TEST(BasicArrow, CountTypes) { ASSERT_OK(CountTypes()); }
