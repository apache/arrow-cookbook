cmake_minimum_required(VERSION 3.19)
project(arrow-cookbook)

set(CMAKE_CXX_STANDARD 17)

# Add googletest
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        e2239ee6043f73722e7aa812a459f54a28552929 # release-1.11.0
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Add Arrow
find_package(Arrow REQUIRED)

# Create test targets
enable_testing()
include(GoogleTest)
add_executable(
        creating_arrow_objects
        creating_arrow_objects.cc
        common.cc
        main.cc
)
target_link_libraries(
        creating_arrow_objects
        arrow_shared
        gtest
)
gtest_discover_tests(creating_arrow_objects)

add_executable(
        basic_arrow
        basic_arrow.cc
        common.cc
        main.cc
)
target_link_libraries(
        basic_arrow
        arrow_shared
        gtest
)
gtest_discover_tests(basic_arrow)
