# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.19)
project(arrow-cookbook)

set(CMAKE_CXX_STANDARD 17)
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libstdc++")
endif()

# Add Arrow and other required packages
find_package(Arrow REQUIRED)
if(NOT ${ARROW_VERSION} VERSION_GREATER "9.0.0")
  get_filename_component(ARROW_CMAKE_BASE_DIR ${Arrow_CONFIG} DIRECTORY)
  list(INSERT CMAKE_MODULE_PATH 0 ${ARROW_CMAKE_BASE_DIR})
endif()
find_package(ArrowDataset REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(Parquet REQUIRED)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_CLANG_TIDY "clang-tidy")
endif()

# Create test targets
enable_testing()

find_package(GTest REQUIRED)
include(GoogleTest)

function(RECIPE TARGET)
    add_executable(
            ${TARGET}
            ${TARGET}.cc
            common.cc
            main.cc
    )
    if(TARGET Arrow::arrow_shared)
        target_link_libraries(
            ${TARGET}
            ArrowDataset::arrow_dataset_shared
            ArrowFlight::arrow_flight_shared GTest::gtest
        )
    else()
        target_link_libraries(parquet_shared INTERFACE arrow_shared)
        target_link_libraries(arrow_dataset_shared INTERFACE parquet_shared)
        target_link_libraries(arrow_flight_shared INTERFACE arrow_shared)
        target_link_libraries(${TARGET} arrow_dataset_shared arrow_flight_shared GTest::gtest)
    endif()
    if (MSVC)
        target_compile_options(${TARGET} PRIVATE /W4 /WX)
    else ()
        target_compile_options(${TARGET} PRIVATE -Wall -Wextra -Wpedantic -Werror)
    endif ()

    gtest_discover_tests(${TARGET})
endfunction()

recipe(basic_arrow)
recipe(creating_arrow_objects)
recipe(datasets)
recipe(flight)


# Add protobuf to flight
find_package(gRPC CONFIG REQUIRED)
find_package(Threads)

set(PROTO_FILES
    protos/helloworld.proto
)

target_link_libraries(flight
    protobuf::libprotobuf
    gRPC::grpc
    gRPC::grpc++
)
target_include_directories(flight PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
protobuf_generate(TARGET flight LANGUAGE cpp PROTOS ${PROTO_FILES})
protobuf_generate(TARGET flight LANGUAGE grpc
                  GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
                  PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}"
                  PROTOS ${PROTO_FILES})
