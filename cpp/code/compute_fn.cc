// ------------------------------
// Dependencies

// standard dependencies
#include <stdint.h>
#include <string>
#include <iostream>

// arrow dependencies
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/util/hashing.h>

#include "common.h"


// >> aliases for types in standard library
using std::shared_ptr;
using std::vector;

// >> commonly used arrow types
//    |> general programming support
using arrow::Result;
using arrow::Status;
using arrow::Datum;

//    |> arrow data types and helpers
using arrow::Int64Builder;
using arrow::Array;
using arrow::ArraySpan;


// >> aliases for types used to define a custom function (e.g. `NamedScalarFn`)
//    |> kernel parameters
using arrow::compute::KernelContext;
using arrow::compute::ExecSpan;
using arrow::compute::ExecResult;

//    |> for defining compute functions and their compute kernels
using arrow::compute::FunctionDoc;
using arrow::compute::InputType;
using arrow::compute::OutputType;
using arrow::compute::Arity;
using arrow::compute::ScalarFunction;

//    |> for adding to a function registry or using `CallFunction`
using arrow::compute::FunctionRegistry;
using arrow::compute::ExecContext;


// ------------------------------
// Structs and Classes

// >> Documentation for a compute function
/**
 * Create a const instance of `FunctionDoc` that contains 3 attributes:
 *  1. Short description
 *  2. Long  description (can be multiple lines, each limited to 78 characters in width)
 *  3. Name of input arguments
 */
const FunctionDoc named_scalar_fn_doc {
   "Unary function that calculates a hash for each element of the input"
  ,("This function uses the xxHash algorithm.\n"
    "The result contains a 64-bit hash value for each input element.")
  ,{ "input_array" }
};


// >> Kernel implementations for a compute function
// StartRecipe("DefineAComputeKernel");
/**
 * Create implementations that will be associated with our compute function. When a
 * compute function is invoked, the compute API framework will delegate execution to an
 * associated kernel that matches: (1) input argument types/shapes and (2) output argument
 * types/shapes.
 *
 * Kernel implementations may be functions or may be methods (functions within a class or
 * struct).
 */
struct NamedScalarFn {

  /**
   * A kernel implementation that expects a single array as input, and outputs an array of
   * int64 values. We write this implementation knowing what function we want to
   * associate it with ("NamedScalarFn"), but that association is made later (see
   * `RegisterScalarFnKernels()` below).
   */
  static Status
  Exec(KernelContext *ctx, const ExecSpan &input_arg, ExecResult *out) {

    // Validate inputs
    if (input_arg.num_values() != 1 or !input_arg[0].is_array()) {
      return Status::Invalid("Unsupported argument types or shape");
    }

    // The input ArraySpan manages data as 3 buffers; the data buffer has index `1`
    constexpr int      bufndx_data = 1;
    const     int64_t *hash_inputs = input_arg[0].array.GetValues<int64_t>(bufndx_data);
    const     auto     input_len   = input_arg[0].array.length;

    // Allocate an Arrow buffer for output
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> hash_buffer,
                          AllocateBuffer(input_len * sizeof(int64_t)));

    // Call hashing function, using both prime multipliers from xxHash
    int64_t *hash_results = reinterpret_cast<int64_t*>(hash_buffer->mutable_data());
    for (int val_ndx = 0; val_ndx < input_len; ++val_ndx) {
      hash_results[val_ndx] = (
          ScalarHelper<int64_t, 0>::ComputeHash(hash_inputs[val_ndx])
        + ScalarHelper<int64_t, 1>::ComputeHash(hash_inputs[val_ndx])
      );
    }

    // Use ArrayData (not ArraySpan) for ownership of result buffer
    out->value = ArrayData{int64(), input_len, {nullptr, std::move(hash_buffer)}};
    return Status::OK();
  }
};
// EndRecipe("DefineAComputeKernel");


// ------------------------------
// Functions


// >> Function registration and kernel association
// StartRecipe("AddKernelToFunction");
/**
 * A convenience function that shows how we construct an instance of `ScalarFunction` that
 * will be registered in a function registry. The instance is constructed with: (1) a
 * unique name ("named_scalar_fn"), (2) an "arity" (`Arity::Unary()`), and (3) an instance
 * of `FunctionDoc`.
 *
 * The function name is used to invoke it from a function registry after it has been
 * registered. The "arity" is the cardinality of the function's parameters--1 parameter is
 * a unary function, 2 parameters is a binary function, etc. Finally, it is helpful to
 * associate the function with documentation, which uses the `FunctionDoc` struct.
 */
shared_ptr<ScalarFunction>
RegisterScalarFnKernels() {
  // Instantiate a function to be registered
  auto fn_named_scalar = std::make_shared<ScalarFunction>(
     "named_scalar_fn"
    ,Arity::Unary()
    ,std::move(named_scalar_fn_doc)
  );

  // Associate a function and kernel using `ScalarFunction::AddKernel()`
  DCHECK_OK(
    fn_named_scalar->AddKernel(
       { InputType(arrow::int64()) }
      ,OutputType(arrow::int64())
      ,NamedScalarFn::Exec
    )
  );

  return fn_named_scalar;
}
// EndRecipe("AddKernelToFunction");


// StartRecipe("AddFunctionToRegistry");
/**
 * A convenience function that shows how we register a custom function with a
 * `FunctionRegistry`. To keep this simple and general, this function takes a pointer to a
 * FunctionRegistry as an input argument, then invokes `FunctionRegistry::AddFunction()`.
 */
void
RegisterNamedScalarFn(FunctionRegistry *registry) {
  // scalar_fn has type: shared_ptr<ScalarFunction>
  auto scalar_fn = RegisterScalarFnKernels();
  DCHECK_OK(registry->AddFunction(std::move(scalar_fn)));
}
// EndRecipe("AddFunctionToRegistry");


// >> Convenience functions
// StartRecipe("InvokeByCallFunction");
/**
 * An optional, convenient invocation function to easily call our compute function. This
 * executes our compute function by invoking `CallFunction` with the name that we used to
 * register the function ("named_scalar_fn" in this case).
 */
ARROW_EXPORT
Result<Datum>
NamedScalarFn(const Datum &input_arg, ExecContext *ctx) {
  auto func_name    = "named_scalar_fn";
  auto result_datum = CallFunction(func_name, { input_arg }, ctx);

  return result_datum;
}
// EndRecipe("InvokeByCallFunction");


Result<shared_ptr<Array>>
BuildIntArray() {
  vector<int64_t> col_vals { 0, 1, 1, 2, 3, 5, 8, 13, 21, 34 };

  Int64Builder builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(col_vals.size()));
  ARROW_RETURN_NOT_OK(builder.AppendValues(col_vals));
  return builder.Finish();
}


class ComputeFunctionTest : public ::testing::Test {};

TEST(ComputeFunctionTest, TestRegisterAndCallFunction) {
  // >> Register the function first
  auto fn_registry = arrow::compute::GetFunctionRegistry();
  RegisterNamedScalarFn(fn_registry);

  // >> Then we can call the function
  StartRecipe("InvokeByConvenienceFunction");
  auto build_result = BuildIntArray();
  if (not build_result.ok()) {
    std::cerr << build_result.status().message() << std::endl;
    return 1;
  }

  Datum col_data { *build_result };
  auto fn_result = NamedScalarFn(col_data);
  if (not fn_result.ok()) {
    std::cerr << fn_result.status().message() << std::endl;
    return 2;
  }

  auto result_data = fn_result->make_array();
  std::cout << "Success:"                      << std::endl;
  std::cout << "\t" << result_data->ToString() << std::endl;
  EndRecipe("InvokeByConvenienceFunction");

  // If we want to peek at the input data
  std::cout << col_data.make_array()->ToString() << std::endl;

  return 0;
}
