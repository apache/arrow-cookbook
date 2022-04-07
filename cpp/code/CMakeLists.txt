cmake_minimum_required(VERSION 3.19)
project(arrow-cookbook)

set(CMAKE_CXX_STANDARD 17)

# Add Arrow
find_package(Arrow REQUIRED COMPONENTS dataset flight parquet)

# You may wish to comment this line out to temporarily ignore pendantic linting errors
set(CMAKE_CXX_CLANG_TIDY "clang-tidy")

# Create test targets
enable_testing()

include(GoogleTest)

function(RECIPE TARGET)
    add_executable(
            ${TARGET}
            ${TARGET}.cc
            common.cc
            main.cc
    )
    target_link_libraries(
            ${TARGET}
            arrow_shared
            arrow_dataset
            arrow_flight
            parquet
            gtest
    )
    if (MSVC)
        target_compile_options(${TARGET} PRIVATE /W4 /WX)
    else ()
        target_compile_options(${TARGET} PRIVATE -Wall -Wextra -Wpedantic -Werror -Wno-unused-parameter)
    endif ()

    gtest_discover_tests(${TARGET})
endfunction()

recipe(basic_arrow)
recipe(creating_arrow_objects)
recipe(datasets)
recipe(flight)


# Add protobuf to flight
find_package(Protobuf REQUIRED)
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