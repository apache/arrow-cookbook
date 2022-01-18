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

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/flight/client.h>
#include <arrow/flight/server.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>

#include "common.h"

class ParquetStorageService : public arrow::flight::FlightServerBase {
 public:
  const arrow::flight::ActionType kActionDropDataset{"drop_dataset",
                                                     "Delete a dataset."};

  explicit ParquetStorageService(std::shared_ptr<arrow::fs::FileSystem> root)
      : root_(std::move(root)) {}

  arrow::Status ListFlights(
      const arrow::flight::ServerCallContext&, const arrow::flight::Criteria*,
      std::unique_ptr<arrow::flight::FlightListing>* listings) {
    arrow::fs::FileSelector selector;
    selector.base_dir = "/";
    ARROW_ASSIGN_OR_RAISE(auto listing, root_->GetFileInfo(selector));

    std::vector<arrow::flight::FlightInfo> flights;
    for (const auto& file_info : listing) {
      if (!file_info.IsFile() || file_info.extension() != "parquet") continue;
      ARROW_ASSIGN_OR_RAISE(auto info, MakeFlightInfo(file_info));
      flights.push_back(std::move(info));
    }

    *listings = std::unique_ptr<arrow::flight::FlightListing>(
        new arrow::flight::SimpleFlightListing(std::move(flights)));
    return arrow::Status::OK();
  }

  arrow::Status GetFlightInfo(
      const arrow::flight::ServerCallContext&,
      const arrow::flight::FlightDescriptor& descriptor,
      std::unique_ptr<arrow::flight::FlightInfo>* info) {
    ARROW_ASSIGN_OR_RAISE(auto file_info, FileInfoFromDescriptor(descriptor));
    ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(file_info));
    *info = std::unique_ptr<arrow::flight::FlightInfo>(
        new arrow::flight::FlightInfo(std::move(flight_info)));
    return arrow::Status::OK();
  }

  arrow::Status DoPut(
      const arrow::flight::ServerCallContext&,
      std::unique_ptr<arrow::flight::FlightMessageReader> reader,
      std::unique_ptr<arrow::flight::FlightMetadataWriter>) {
    ARROW_ASSIGN_OR_RAISE(auto file_info,
                          FileInfoFromDescriptor(reader->descriptor()));
    ARROW_ASSIGN_OR_RAISE(auto sink, root_->OpenOutputStream(file_info.path()));
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(reader->ReadAll(&table));

    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), sink, /*chunk_size=*/65536));
    return arrow::Status::OK();
  }

  arrow::Status DoGet(
      const arrow::flight::ServerCallContext&,
      const arrow::flight::Ticket& request,
      std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
    ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(request.ticket));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(
        std::move(input), arrow::default_memory_pool(), &reader));

    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(reader->ReadTable(&table));
    // Note that we can't directly pass TableBatchReader to
    // RecordBatchStream because TableBatchReader keeps a non-owning
    // reference to the underlying Table, which would then get freed
    // when we exit this function
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    arrow::TableBatchReader batch_reader(*table);
    ARROW_RETURN_NOT_OK(batch_reader.ReadAll(&batches));

    ARROW_ASSIGN_OR_RAISE(
        auto owning_reader,
        arrow::RecordBatchReader::Make(std::move(batches), table->schema()));
    *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
        new arrow::flight::RecordBatchStream(owning_reader));

    return arrow::Status::OK();
  }

  arrow::Status ListActions(
      const arrow::flight::ServerCallContext&,
      std::vector<arrow::flight::ActionType>* actions) override {
    *actions = {kActionDropDataset};
    return arrow::Status::OK();
  }

  arrow::Status DoAction(const arrow::flight::ServerCallContext&,
                         const arrow::flight::Action& action,
                         std::unique_ptr<arrow::flight::ResultStream>* result) {
    if (action.type == kActionDropDataset.type) {
      *result = std::unique_ptr<arrow::flight::ResultStream>(
          new arrow::flight::SimpleResultStream({}));
      return DoActionDropDataset(action.body->ToString());
    }
    return arrow::Status::NotImplemented("Unknown action type: ", action.type);
  }

 private:
  arrow::Result<arrow::flight::FlightInfo> MakeFlightInfo(
      const arrow::fs::FileInfo& file_info) {
    ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(file_info));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(
        std::move(input), arrow::default_memory_pool(), &reader));

    std::shared_ptr<arrow::Schema> schema;
    ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));

    auto descriptor =
        arrow::flight::FlightDescriptor::Path({file_info.base_name()});

    arrow::flight::FlightEndpoint endpoint;
    endpoint.ticket.ticket = file_info.base_name();
    arrow::flight::Location location;
    ARROW_RETURN_NOT_OK(
        arrow::flight::Location::ForGrpcTcp("localhost", port(), &location));
    endpoint.locations.push_back(location);

    int64_t total_records = reader->parquet_reader()->metadata()->num_rows();
    int64_t total_bytes = file_info.size();

    return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint},
                                           total_records, total_bytes);
  }

  arrow::Result<arrow::fs::FileInfo> FileInfoFromDescriptor(
      const arrow::flight::FlightDescriptor& descriptor) {
    if (descriptor.type != arrow::flight::FlightDescriptor::PATH) {
      return arrow::Status::Invalid("Must provide PATH-type FlightDescriptor");
    } else if (descriptor.path.size() != 1) {
      return arrow::Status::Invalid(
          "Must provide PATH-type FlightDescriptor with one path component");
    }
    return root_->GetFileInfo(descriptor.path[0]);
  }

  arrow::Status DoActionDropDataset(const std::string& key) {
    return root_->DeleteFile(key);
  }

  std::shared_ptr<arrow::fs::FileSystem> root_;
};  // end ParquetStorageService

TEST(ParquetStorageServiceTest, PutGetDelete) {
  StartRecipe("ParquetStorageService::StartServer");
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  ASSERT_OK(fs->CreateDir("./flight_datasets/"));
  auto root =
      std::make_shared<arrow::fs::SubTreeFileSystem>("./flight_datasets/", fs);

  arrow::flight::Location server_location;
  ASSERT_OK(
      arrow::flight::Location::ForGrpcTcp("0.0.0.0", 0, &server_location));

  arrow::flight::FlightServerOptions options(server_location);
  auto server = std::unique_ptr<arrow::flight::FlightServerBase>(
      new ParquetStorageService(std::move(root)));
  ASSERT_OK(server->Init(options));
  rout << "Listening on port " << server->port() << std::endl;
  EndRecipe("ParquetStorageService::StartServer");

  StartRecipe("ParquetStorageService::Connect");
  arrow::flight::Location location;
  ASSERT_OK(arrow::flight::Location::ForGrpcTcp("localhost", server->port(),
                                                &location));

  std::unique_ptr<arrow::flight::FlightClient> client;
  ASSERT_OK(arrow::flight::FlightClient::Connect(location, &client));
  rout << "Connected to " << location.ToString() << std::endl;
  EndRecipe("ParquetStorageService::Connect");

  StartRecipe("ParquetStorageService::DoPut");
  // Open example data file to upload
  ASSERT_OK_AND_ASSIGN(std::string airquality_path,
                       FindTestDataFile("airquality.parquet"));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::io::RandomAccessFile> input,
                       fs->OpenInputFile(airquality_path));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ASSERT_OK(parquet::arrow::OpenFile(std::move(input),
                                     arrow::default_memory_pool(), &reader));

  auto descriptor =
      arrow::flight::FlightDescriptor::Path({"airquality.parquet"});
  std::shared_ptr<arrow::Schema> schema;
  ASSERT_OK(reader->GetSchema(&schema));

  // Start the RPC call
  std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
  std::unique_ptr<arrow::flight::FlightMetadataReader> metadata_reader;
  ASSERT_OK(client->DoPut(descriptor, schema, &writer, &metadata_reader));

  // Upload data
  std::shared_ptr<arrow::RecordBatchReader> batch_reader;
  std::vector<int> row_groups(reader->num_row_groups());
  std::iota(row_groups.begin(), row_groups.end(), 0);
  ASSERT_OK(reader->GetRecordBatchReader(row_groups, &batch_reader));
  int64_t batches = 0;
  while (true) {
    ASSERT_OK_AND_ASSIGN(auto batch, batch_reader->Next());
    if (!batch) break;
    ASSERT_OK(writer->WriteRecordBatch(*batch));
    batches++;
  }

  ASSERT_OK(writer->Close());
  rout << "Wrote " << batches << " batches" << std::endl;
  EndRecipe("ParquetStorageService::DoPut");

  StartRecipe("ParquetStorageService::GetFlightInfo");
  std::unique_ptr<arrow::flight::FlightInfo> flight_info;
  ASSERT_OK(client->GetFlightInfo(descriptor, &flight_info));
  rout << flight_info->descriptor().ToString() << std::endl;
  rout << "=== Schema ===" << std::endl;
  std::shared_ptr<arrow::Schema> info_schema;
  arrow::ipc::DictionaryMemo dictionary_memo;
  ASSERT_OK(flight_info->GetSchema(&dictionary_memo, &info_schema));
  rout << info_schema->ToString() << std::endl;
  rout << "==============" << std::endl;
  EndRecipe("ParquetStorageService::GetFlightInfo");

  StartRecipe("ParquetStorageService::DoGet");
  std::unique_ptr<arrow::flight::FlightStreamReader> stream;
  ASSERT_OK(client->DoGet(flight_info->endpoints()[0].ticket, &stream));
  std::shared_ptr<arrow::Table> table;
  ASSERT_OK(stream->ReadAll(&table));
  arrow::PrettyPrintOptions print_options(/*indent=*/0, /*window=*/2);
  arrow::PrettyPrint(*table, print_options, &rout);
  EndRecipe("ParquetStorageService::DoGet");

  StartRecipe("ParquetStorageService::DoAction");
  arrow::flight::Action action{"drop_dataset",
                               arrow::Buffer::FromString("airquality.parquet")};
  std::unique_ptr<arrow::flight::ResultStream> results;
  ASSERT_OK(client->DoAction(action, &results));
  rout << "Deleted dataset" << std::endl;
  EndRecipe("ParquetStorageService::DoAction");

  StartRecipe("ParquetStorageService::ListFlights");
  std::unique_ptr<arrow::flight::FlightListing> listing;
  ASSERT_OK(client->ListFlights(&listing));
  while (true) {
    std::unique_ptr<arrow::flight::FlightInfo> flight_info;
    ASSERT_OK(listing->Next(&flight_info));
    if (!flight_info) break;
    rout << flight_info->descriptor().ToString() << std::endl;
    rout << "=== Schema ===" << std::endl;
    std::shared_ptr<arrow::Schema> info_schema;
    arrow::ipc::DictionaryMemo dictionary_memo;
    ASSERT_OK(flight_info->GetSchema(&dictionary_memo, &info_schema));
    rout << info_schema->ToString() << std::endl;
    rout << "==============" << std::endl;
  }
  rout << "End of listing" << std::endl;
  EndRecipe("ParquetStorageService::ListFlights");

  StartRecipe("ParquetStorageService::StopServer");
  ASSERT_OK(server->Shutdown());
  rout << "Server shut down successfully" << std::endl;
  EndRecipe("ParquetStorageService::StopServer");
}
