cmake_minimum_required(VERSION 3.19)
project(arrow-cookbook)

set(CMAKE_CXX_STANDARD 17)
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libstdc++")
endif()

# Add googletest
include(FetchContent)
FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG e2239ee6043f73722e7aa812a459f54a28552929 # release-1.11.0
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Add Arrow
find_package(Arrow REQUIRED COMPONENTS dataset parquet)
get_cmake_property(_variableNames VARIABLES)
list (SORT _variableNames)
foreach (_variableName ${_variableNames})
    message(STATUS "${_variableName}=${${_variableName}}")
endforeach()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_CLANG_TIDY "clang-tidy")
endif()

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
            parquet
            gtest
    )
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
