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

#include <random>

#include "common.h"

arrow::Status CreatingArrays() {
  StartRecipe("CreatingArrays");
  arrow::Int32Builder builder;
  ARROW_RETURN_NOT_OK(builder.Append(1));
  ARROW_RETURN_NOT_OK(builder.Append(2));
  ARROW_RETURN_NOT_OK(builder.Append(3));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> arr, builder.Finish())
  rout << arr->ToString() << std::endl;
  EndRecipe("CreatingArrays");
  return arrow::Status::OK();
}

arrow::Status CreatingArraysPtr() {
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
  EndRecipe("CreatingArraysPtr");
  return arrow::Status::OK();
}

/// \brief Generate random record batches for a given schema
///
/// For demonstration purposes, this only covers DoubleType and ListType
class RandomBatchGenerator {
 public:
  std::shared_ptr<arrow::Schema> schema;

  RandomBatchGenerator(std::shared_ptr<arrow::Schema> schema) : schema(schema){};

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Generate(int32_t num_rows) {
    num_rows_ = num_rows;
    for (std::shared_ptr<arrow::Field> field : schema->fields()) {
      ARROW_RETURN_NOT_OK(arrow::VisitTypeInline(*field->type(), this));
    }

    return arrow::RecordBatch::Make(schema, num_rows, arrays_);
  }

  // Default implementation
  arrow::Status Visit(const arrow::DataType& type) {
    return arrow::Status::NotImplemented("Generating data for", type.ToString());
  }

  arrow::Status Visit(const arrow::DoubleType&) {
    auto builder = arrow::DoubleBuilder();
    std::normal_distribution<> d{/*mean=*/5.0, /*stddev=*/2.0};
    for (int32_t i = 0; i < num_rows_; ++i) {
      builder.Append(d(gen_));
    }
    ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
    arrays_.push_back(array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::ListType& type) {
    // Generate offsets first, which determines number of values in sub-array
    std::poisson_distribution<> d{/*mean=*/4};
    auto builder = arrow::Int32Builder();
    builder.Append(0);
    int32_t last_val = 0;
    for (int32_t i = 0; i < num_rows_; ++i) {
      last_val += d(gen_);
      builder.Append(last_val);
    }
    ARROW_ASSIGN_OR_RAISE(auto offsets, builder.Finish());

    // Since children of list has a new length, will use a new generator
    RandomBatchGenerator value_gen(arrow::schema({arrow::field("x", type.value_type())}));
    // Last index from the offsets array becomes the length of the sub-array
    ARROW_ASSIGN_OR_RAISE(auto inner_batch, value_gen.Generate(last_val));
    std::shared_ptr<arrow::Array> values = inner_batch->column(0);

    ARROW_ASSIGN_OR_RAISE(auto array,
                          arrow::ListArray::FromArrays(*offsets.get(), *values.get()));
    arrays_.push_back(array);

    return arrow::Status::OK();
  }

 protected:
  std::random_device rd_{};
  std::mt19937 gen_{rd_()};
  std::vector<std::shared_ptr<arrow::Array>> arrays_;
  int32_t num_rows_;
};  // RandomBatchGenerator

arrow::Status GenerateRandomData() {
  StartRecipe("GenerateRandomData");
  std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field("x", arrow::float64()),
                     arrow::field("y", arrow::list(arrow::float64()))});

  RandomBatchGenerator generator(schema);
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch, generator.Generate(5));

  rout << "Created batch: \n" << batch->ToString();

  // Consider using ValidateFull to check correctness
  ARROW_RETURN_NOT_OK(batch->ValidateFull());

  EndRecipe("GenerateRandomData");
  EXPECT_EQ(batch->num_rows(), 5);

  return arrow::Status::OK();
}

TEST(CreatingArrowObjects, CreatingArraysTest) { ASSERT_OK(CreatingArrays()); }
TEST(CreatingArrowObjects, CreatingArraysPtrTest) { ASSERT_OK(CreatingArraysPtr()); }
TEST(CreatingArrowObjects, GeneratingRandomData) { ASSERT_OK(GenerateRandomData()); }
