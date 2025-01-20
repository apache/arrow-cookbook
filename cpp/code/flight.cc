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
#include <gmock/gmock.h>
#include <grpc++/grpc++.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <protos/helloworld.grpc.pb.h>
#include <protos/helloworld.pb.h>

#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>

#include "common.h"

class ParquetStorageService : public arrow::flight::FlightServerBase {
 public:
  const arrow::flight::ActionType kActionDropDataset{"drop_dataset", "Delete a dataset."};

  explicit ParquetStorageService(std::shared_ptr<arrow::fs::FileSystem> root)
      : root_(std::move(root)) {}

  arrow::Status ListFlights(
      const arrow::flight::ServerCallContext&, const arrow::flight::Criteria*,
      std::unique_ptr<arrow::flight::FlightListing>* listings) override {
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

  arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext&,
                              const arrow::flight::FlightDescriptor& descriptor,
                              std::unique_ptr<arrow::flight::FlightInfo>* info) override {
    ARROW_ASSIGN_OR_RAISE(auto file_info, FileInfoFromDescriptor(descriptor));
    ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(file_info));
    *info = std::unique_ptr<arrow::flight::FlightInfo>(
        new arrow::flight::FlightInfo(std::move(flight_info)));
    return arrow::Status::OK();
  }

  arrow::Status DoPut(const arrow::flight::ServerCallContext&,
                      std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                      std::unique_ptr<arrow::flight::FlightMetadataWriter>) override {
    ARROW_ASSIGN_OR_RAISE(auto file_info, FileInfoFromDescriptor(reader->descriptor()));
    ARROW_ASSIGN_OR_RAISE(auto sink, root_->OpenOutputStream(file_info.path()));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, reader->ToTable());

    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                                   sink, /*chunk_size=*/65536));
    return arrow::Status::OK();
  }

  arrow::Status DoGet(const arrow::flight::ServerCallContext&,
                      const arrow::flight::Ticket& request,
                      std::unique_ptr<arrow::flight::FlightDataStream>* stream) override {
    ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(request.ticket));
    ARROW_ASSIGN_OR_RAISE(
        auto reader,
        parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool()));

    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(reader->ReadTable(&table));
    // Note that we can't directly pass TableBatchReader to
    // RecordBatchStream because TableBatchReader keeps a non-owning
    // reference to the underlying Table, which would then get freed
    // when we exit this function
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    arrow::TableBatchReader batch_reader(*table);
    ARROW_ASSIGN_OR_RAISE(batches, batch_reader.ToRecordBatches());

    ARROW_ASSIGN_OR_RAISE(auto owning_reader, arrow::RecordBatchReader::Make(
                                                  std::move(batches), table->schema()));
    *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
        new arrow::flight::RecordBatchStream(owning_reader));

    return arrow::Status::OK();
  }

  arrow::Status ListActions(const arrow::flight::ServerCallContext&,
                            std::vector<arrow::flight::ActionType>* actions) override {
    *actions = {kActionDropDataset};
    return arrow::Status::OK();
  }

  arrow::Status DoAction(const arrow::flight::ServerCallContext&,
                         const arrow::flight::Action& action,
                         std::unique_ptr<arrow::flight::ResultStream>* result) override {
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
    ARROW_ASSIGN_OR_RAISE(
        auto reader,
        parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool()));

    std::shared_ptr<arrow::Schema> schema;
    ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));

    auto descriptor = arrow::flight::FlightDescriptor::Path({file_info.base_name()});

    arrow::flight::FlightEndpoint endpoint;
    endpoint.ticket.ticket = file_info.base_name();
    arrow::flight::Location location;
    ARROW_ASSIGN_OR_RAISE(location,
        arrow::flight::Location::ForGrpcTcp("localhost", port()));
    endpoint.locations.push_back(location);

    int64_t total_records = reader->parquet_reader()->metadata()->num_rows();
    int64_t total_bytes = file_info.size();

    return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint}, total_records,
                                           total_bytes);
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

class HelloWorldServiceImpl : public HelloWorldService::Service {
  grpc::Status SayHello(grpc::ServerContext*, const HelloRequest* request,
                        HelloResponse* reply) override {
    const std::string& name = request->name();
    if (name.empty()) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Must provide a name!");
    }
    reply->set_reply("Hello, " + name);
    return grpc::Status::OK;
  }
};  // end HelloWorldServiceImpl

arrow::Status TestPutGetDelete() {
  StartRecipe("ParquetStorageService::StartServer");
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  ARROW_RETURN_NOT_OK(fs->CreateDir("./flight_datasets/"));
  ARROW_RETURN_NOT_OK(fs->DeleteDirContents("./flight_datasets/"));
  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./flight_datasets/", fs);

  arrow::flight::Location server_location;
  ARROW_ASSIGN_OR_RAISE(server_location,
      arrow::flight::Location::ForGrpcTcp("0.0.0.0", 0));

  arrow::flight::FlightServerOptions options(server_location);
  auto server = std::unique_ptr<arrow::flight::FlightServerBase>(
      new ParquetStorageService(std::move(root)));
  ARROW_RETURN_NOT_OK(server->Init(options));
  rout << "Listening on port " << server->port() << std::endl;
  EndRecipe("ParquetStorageService::StartServer");

  StartRecipe("ParquetStorageService::Connect");
  arrow::flight::Location location;
  ARROW_ASSIGN_OR_RAISE(location,
      arrow::flight::Location::ForGrpcTcp("localhost", server->port()));

  std::unique_ptr<arrow::flight::FlightClient> client;
  ARROW_ASSIGN_OR_RAISE(client, arrow::flight::FlightClient::Connect(location));
  rout << "Connected to " << location.ToString() << std::endl;
  EndRecipe("ParquetStorageService::Connect");

  StartRecipe("ParquetStorageService::DoPut");
  // Open example data file to upload
  ARROW_ASSIGN_OR_RAISE(std::string airquality_path,
                        FindTestDataFile("airquality.parquet"));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::io::RandomAccessFile> input,
                        fs->OpenInputFile(airquality_path));
  ARROW_ASSIGN_OR_RAISE(auto reader, parquet::arrow::OpenFile(
                                         std::move(input), arrow::default_memory_pool()));

  auto descriptor = arrow::flight::FlightDescriptor::Path({"airquality.parquet"});
  std::shared_ptr<arrow::Schema> schema;
  ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));

  // Start the RPC call
  std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
  std::unique_ptr<arrow::flight::FlightMetadataReader> metadata_reader;
  ARROW_ASSIGN_OR_RAISE(auto put_stream, client->DoPut(descriptor, schema));
  writer = std::move(put_stream.writer);
  metadata_reader = std::move(put_stream.reader);

  // Upload data
  std::shared_ptr<arrow::RecordBatchReader> batch_reader;
  std::vector<int> row_groups(reader->num_row_groups());
  std::iota(row_groups.begin(), row_groups.end(), 0);
  ARROW_RETURN_NOT_OK(reader->GetRecordBatchReader(row_groups, &batch_reader));
  int64_t batches = 0;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_reader->Next());
    if (!batch) break;
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    batches++;
  }

  ARROW_RETURN_NOT_OK(writer->Close());
  rout << "Wrote " << batches << " batches" << std::endl;
  EndRecipe("ParquetStorageService::DoPut");

  StartRecipe("ParquetStorageService::GetFlightInfo");
  std::unique_ptr<arrow::flight::FlightInfo> flight_info;
  ARROW_ASSIGN_OR_RAISE(flight_info, client->GetFlightInfo(descriptor));
  rout << flight_info->descriptor().ToString() << std::endl;
  rout << "=== Schema ===" << std::endl;
  std::shared_ptr<arrow::Schema> info_schema;
  arrow::ipc::DictionaryMemo dictionary_memo;
  ARROW_ASSIGN_OR_RAISE(info_schema, flight_info->GetSchema(&dictionary_memo));
  rout << info_schema->ToString() << std::endl;
  rout << "==============" << std::endl;
  EndRecipe("ParquetStorageService::GetFlightInfo");

  StartRecipe("ParquetStorageService::DoGet");
  std::unique_ptr<arrow::flight::FlightStreamReader> stream;
  ARROW_ASSIGN_OR_RAISE(stream, client->DoGet(flight_info->endpoints()[0].ticket));
  std::shared_ptr<arrow::Table> table;
  ARROW_ASSIGN_OR_RAISE(table, stream->ToTable());
  arrow::PrettyPrintOptions print_options(/*indent=*/0, /*window=*/2);
  ARROW_RETURN_NOT_OK(arrow::PrettyPrint(*table, print_options, &rout));
  EndRecipe("ParquetStorageService::DoGet");

  StartRecipe("ParquetStorageService::DoAction");
  arrow::flight::Action action{"drop_dataset",
                               arrow::Buffer::FromString("airquality.parquet")};
  std::unique_ptr<arrow::flight::ResultStream> results;
  ARROW_ASSIGN_OR_RAISE(results, client->DoAction(action));
  rout << "Deleted dataset" << std::endl;
  EndRecipe("ParquetStorageService::DoAction");

  StartRecipe("ParquetStorageService::ListFlights");
  std::unique_ptr<arrow::flight::FlightListing> listing;
  ARROW_ASSIGN_OR_RAISE(listing, client->ListFlights());
  while (true) {
    std::unique_ptr<arrow::flight::FlightInfo> flight_info;
    ARROW_ASSIGN_OR_RAISE(flight_info, listing->Next());
    if (!flight_info) break;
    rout << flight_info->descriptor().ToString() << std::endl;
    rout << "=== Schema ===" << std::endl;
    std::shared_ptr<arrow::Schema> info_schema;
    arrow::ipc::DictionaryMemo dictionary_memo;
    ARROW_ASSIGN_OR_RAISE(info_schema, flight_info->GetSchema(&dictionary_memo));
    rout << info_schema->ToString() << std::endl;
    rout << "==============" << std::endl;
  }
  rout << "End of listing" << std::endl;
  EndRecipe("ParquetStorageService::ListFlights");

  StartRecipe("ParquetStorageService::StopServer");
  ARROW_RETURN_NOT_OK(server->Shutdown());
  rout << "Server shut down successfully" << std::endl;
  EndRecipe("ParquetStorageService::StopServer");
  return arrow::Status::OK();
}

arrow::Status TestClientOptions() {
  // Set up server as usual
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  ARROW_RETURN_NOT_OK(fs->CreateDir("./flight_datasets/"));
  ARROW_RETURN_NOT_OK(fs->DeleteDirContents("./flight_datasets/"));
  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./flight_datasets/", fs);

  arrow::flight::Location server_location;
  ARROW_ASSIGN_OR_RAISE(server_location,
      arrow::flight::Location::ForGrpcTcp("0.0.0.0", 0));

  arrow::flight::FlightServerOptions options(server_location);
  auto server = std::unique_ptr<arrow::flight::FlightServerBase>(
      new ParquetStorageService(std::move(root)));
  ARROW_RETURN_NOT_OK(server->Init(options));

  StartRecipe("TestClientOptions::Connect");
  auto client_options = arrow::flight::FlightClientOptions::Defaults();
  // Set a very low limit at the gRPC layer to fail all calls
  client_options.generic_options.emplace_back(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, 2);

  arrow::flight::Location location;
  ARROW_ASSIGN_OR_RAISE(location,
      arrow::flight::Location::ForGrpcTcp("localhost", server->port()));

  std::unique_ptr<arrow::flight::FlightClient> client;
  // pass client_options into Connect()
  ARROW_ASSIGN_OR_RAISE(client,
      arrow::flight::FlightClient::Connect(location, client_options));
  rout << "Connected to " << location.ToString() << std::endl;
  EndRecipe("TestClientOptions::Connect");

  auto descriptor = arrow::flight::FlightDescriptor::Path({"airquality.parquet"});
  return client->GetFlightInfo(descriptor).status();
}

arrow::Status TestCustomGrpcImpl() {
  // Build flight service as usual
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  ARROW_RETURN_NOT_OK(fs->CreateDir("./flight_datasets/"));
  ARROW_RETURN_NOT_OK(fs->DeleteDirContents("./flight_datasets/"));
  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./flight_datasets/", fs);

  StartRecipe("CustomGrpcImpl::StartServer");
  arrow::flight::Location server_location;
  ARROW_ASSIGN_OR_RAISE(server_location,
      arrow::flight::Location::ForGrpcTcp("0.0.0.0", 5000));

  arrow::flight::FlightServerOptions options(server_location);
  auto server = std::unique_ptr<arrow::flight::FlightServerBase>(
      new ParquetStorageService(std::move(root)));

  // Create hello world service
  HelloWorldServiceImpl grpc_service;

  // Use builder_hook to register grpc service
  options.builder_hook = [&](void* raw_builder) {
    auto* builder = reinterpret_cast<grpc::ServerBuilder*>(raw_builder);
    builder->RegisterService(&grpc_service);
  };

  ARROW_RETURN_NOT_OK(server->Init(options));
  rout << "Listening on port " << server->port() << std::endl;
  EndRecipe("CustomGrpcImpl::StartServer");

  StartRecipe("CustomGrpcImpl::CreateClient");
  auto client_channel =
      grpc::CreateChannel("0.0.0.0:5000", grpc::InsecureChannelCredentials());

  auto stub = HelloWorldService::NewStub(client_channel);

  grpc::ClientContext context;
  HelloRequest request;
  request.set_name("Arrow User");
  HelloResponse response;
  grpc::Status status = stub->SayHello(&context, request, &response);
  if (!status.ok()) {
    return arrow::Status::IOError(status.error_message());
  }
  rout << response.reply();

  EndRecipe("CustomGrpcImpl::CreateClient");
  return arrow::Status::OK();
}

TEST(ParquetStorageServiceTest, PutGetDelete) { ASSERT_OK(TestPutGetDelete()); }
TEST(ParquetStorageServiceTest, TestClientOptions) {
  auto status = TestClientOptions();
  ASSERT_EQ(status.code(), arrow::StatusCode::Invalid);
  ASSERT_THAT(status.message(), testing::HasSubstr("resource exhausted"));
}
TEST(ParquetStorageServiceTest, TestCustomGrpcImpl) { ASSERT_OK(TestCustomGrpcImpl()); }
