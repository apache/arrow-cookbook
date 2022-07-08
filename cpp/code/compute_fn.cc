// ------------------------------
// Dependencies

// standard dependencies
#include <stdint.h>
#include <string>
#include <iostream>

// arrow dependencies
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec/key_hash.h>

#include "common.h"


// >> aliases for types in standard library
using std::shared_ptr;
using std::vector;

// arrow util types
using arrow::Result;
using arrow::Status;
using arrow::Datum;

// arrow data types and helpers
using arrow::UInt32Builder;
using arrow::Int32Builder;

using arrow::Array;
using arrow::ArraySpan;


// aliases for types used in `NamedScalarFn`
//    |> kernel parameters
using arrow::compute::KernelContext;
using arrow::compute::ExecSpan;
using arrow::compute::ExecResult;

//    |> other context types
using arrow::compute::ExecContext;
using arrow::compute::LightContext;

//    |> common types for compute functions
using arrow::compute::FunctionRegistry;
using arrow::compute::FunctionDoc;
using arrow::compute::InputType;
using arrow::compute::OutputType;
using arrow::compute::Arity;

//    |> the "kind" of function we want
using arrow::compute::ScalarFunction;

//    |> structs and classes for hashing
using arrow::util::MiniBatch;
using arrow::util::TempVectorStack;

using arrow::compute::KeyColumnArray;
using arrow::compute::Hashing32;

//    |> functions used for hashing
using arrow::compute::ColumnArrayFromArrayData;


// ------------------------------
// Structs and Classes

// >> Documentation for a compute function
/**
 * Create a const instance of `FunctionDoc` that contains 3 attributes:
 *  1. Short description
 *  2. Long  description (limited to 78 characters)
 *  3. Name of input arguments
 */
const FunctionDoc named_scalar_fn_doc {
   "Unary function that calculates a hash for each row of the input"
  ,"This function uses an xxHash-like algorithm which produces 32-bit hashes."
  ,{ "input_array" }
};


// >> Kernel implementations for a compute function
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
   * uint32 values. We write this implementation knowing what function we want to
   * associate it with ("NamedScalarFn"), but that association is made later (see
   * `RegisterScalarFnKernels()` below).
   */
  static Status
  Exec(KernelContext *ctx, const ExecSpan &input_arg, ExecResult *out) {
    StartRecipe("DefineAComputeKernel");

    if (input_arg.num_values() != 1 or not input_arg[0].is_array()) {
      return Status::Invalid("Unsupported argument types or shape");
    }

    // >> Initialize stack-based memory allocator with an allocator and memory size
    TempVectorStack stack_memallocator;
    auto            input_dtype_width = input_arg[0].type()->bit_width();
    if (input_dtype_width > 0) {
      ARROW_RETURN_NOT_OK(
        stack_memallocator.Init(
           ctx->exec_context()->memory_pool()
          ,input_dtype_width * max_batchsize
        )
      );
    }

    // >> Prepare input data structure for propagation to hash function
    // NOTE: "start row index" and "row count" can potentially be options in the future
    ArraySpan hash_input    = input_arg[0].array;
    int64_t   hash_startrow = 0;
    int64_t   hash_rowcount = hash_input.length;
    ARROW_ASSIGN_OR_RAISE(
       KeyColumnArray input_keycol
      ,ColumnArrayFromArrayData(hash_input.ToArrayData(), hash_startrow, hash_rowcount)
    );

    // >> Call hashing function
    vector<uint32_t> hash_results;
    hash_results.resize(hash_input.length);

    LightContext hash_ctx;
    hash_ctx.hardware_flags = ctx->exec_context()->cpu_info()->hardware_flags();
    hash_ctx.stack          = &stack_memallocator;

    Hashing32::HashMultiColumn({ input_keycol }, &hash_ctx, hash_results.data());

    // >> Prepare results of hash function for kernel output argument
    UInt32Builder builder;
    builder.Reserve(hash_results.size());
    builder.AppendValues(hash_results);

    ARROW_ASSIGN_OR_RAISE(auto result_array, builder.Finish());
    out->value = result_array->data();

    EndRecipe("DefineAComputeKernel");
    return Status::OK();
  }


  static constexpr uint32_t max_batchsize = MiniBatch::kMiniBatchLength;
};


// ------------------------------
// Functions


// >> Function registration and kernel association
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
  StartRecipe("AddKernelsToFunction");
  // Instantiate a function to be registered
  auto fn_named_scalar = std::make_shared<ScalarFunction>(
     "named_scalar_fn"
    ,Arity::Unary()
    ,std::move(named_scalar_fn_doc)
  );

  // Associate a kernel implementation with the function using
  // `ScalarFunction::AddKernel()`
  DCHECK_OK(
    fn_named_scalar->AddKernel(
       { InputType(arrow::int32()) }
      ,OutputType(arrow::uint32())
      ,NamedScalarFn::Exec
    )
  );

  EndRecipe("AddKernelsToFunction");
  return fn_named_scalar;
}


/**
 * A convenience function that shows how we register a custom function with a
 * `FunctionRegistry`. To keep this simple and general, this function takes a pointer to a
 * FunctionRegistry as an input argument, then invokes `FunctionRegistry::AddFunction()`.
 */
void
RegisterNamedScalarFn(FunctionRegistry *registry) {
  auto scalar_fn = RegisterScalarFnKernels();
  DCHECK_OK(registry->AddFunction(std::move(scalar_fn)));
}


// >> Convenience functions
/**
 * An optional convenience function to easily invoke our compute function. This executes
 * our compute function by invoking `CallFunction` with the name that we used to register
 * the function ("named_scalar_fn" in this case).
 */
ARROW_EXPORT
Result<Datum>
NamedScalarFn(const Datum &input_arg, ExecContext *ctx) {
  auto func_name = "named_scalar_fn";
  return CallFunction(func_name, { input_arg }, ctx);
}


Result<shared_ptr<Array>>
BuildIntArray() {
  vector<int32_t> col_vals { 0, 1, 1, 2, 3, 5, 8, 13, 21, 34 };

  Int32Builder builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(col_vals.size()));
  ARROW_RETURN_NOT_OK(builder.AppendValues(col_vals));
  return builder.Finish();
}


class ComputeFunctionTest : public ::testing::Test {};

TEST(ComputeFunctionTest, TestRegisterAndCallFunction) {
  // >> Construct some test data
  auto build_result = BuildIntArray();
  if (not build_result.ok()) {
    std::cerr << build_result.status().message() << std::endl;
    return 1;
  }

  // >> Peek at the data
  auto col_vals = *build_result;
  std::cout << col_vals->ToString() << std::endl;

  // >> Invoke compute function
  StartRecipe("RegisterAndCallComputeFunction");
  //  |> First, register
  auto fn_registry = arrow::compute::GetFunctionRegistry();
  RegisterNamedScalarFn(fn_registry);


  //  |> Then, invoke
  Datum col_as_datum { col_vals };
  auto fn_result = NamedScalarFn(col_as_datum);
  if (not fn_result.ok()) {
    std::cerr << fn_result.status().message() << std::endl;
    return 2;
  }

  auto result_data = fn_result->make_array();
  std::cout << "Success:"                      << std::endl;
  std::cout << "\t" << result_data->ToString() << std::endl;

  EndRecipe("RegisterAndCallComputeFunction");
  return 0;
}
