#!/usr/bin/env python3
"""
Integration test for sparrow-pycapsule with Polars.

This test demonstrates bidirectional data exchange between sparrow (C++) and Polars (Python)
using the Arrow C Data Interface. The C++ library returns raw pointers, and Python creates
the PyCapsules to avoid Python C API calls from ctypes-loaded libraries.
"""

import sys
import ctypes
import os
from pathlib import Path
import polars as pl
import pyarrow as pa

# Set RTLD_GLOBAL and RTLD_NOW flags before loading any libraries
# This ensures that symbols are shared globally
if hasattr(sys, 'setdlopenflags'):
    sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_NOW)


def find_library():
    """Find the sparrow-pycapsule shared library."""
    # First check environment variable
    env_path = os.environ.get('SPARROW_PYCAPSULE_LIB_PATH')
    if env_path:
        lib_path = Path(env_path)
        if lib_path.exists():
            return str(lib_path)
        else:
            raise FileNotFoundError(
                f"SPARROW_PYCAPSULE_LIB_PATH points to non-existent file: {env_path}"
            )
    
    # Fallback: try to find the library in the build directory
    build_dir = Path(__file__).parent.parent / "build" / "bin"
    
    # Check different build types and platforms
    possible_paths = [
        build_dir / "Debug" / "libsparrow-pycapsule.so",
        build_dir / "Release" / "libsparrow-pycapsule.so",
        build_dir / "Debug" / "libsparrow-pycapsule.dylib",
        build_dir / "Release" / "libsparrow-pycapsule.dylib",
        build_dir / "Debug" / "sparrow-pycapsule.dll",
        build_dir / "Release" / "sparrow-pycapsule.dll",
    ]
    
    for path in possible_paths:
        if path.exists():
            return str(path)
    
    raise FileNotFoundError(
        f"Could not find sparrow-pycapsule library. "
        f"Set SPARROW_PYCAPSULE_LIB_PATH environment variable or build the project first. "
        f"Searched in: {build_dir}"
    )


def load_test_helper_library():
    """Load the C++ test helper library."""
    # First, load sparrow-pycapsule to ensure it's available
    main_lib_path = find_library()
    ctypes.CDLL(main_lib_path)  # Just load it, RTLD_GLOBAL is already set
    
    # Then load the test helper library
    env_path = os.environ.get('TEST_POLARS_HELPER_LIB_PATH')
    if env_path:
        lib_path = Path(env_path)
        if lib_path.exists():
            lib = ctypes.CDLL(str(lib_path))
            # Initialize Python in the C++ library
            lib.init_python()
            
            # Set up function signatures for pointer-based API
            lib.create_test_array_as_pointers.argtypes = [
                ctypes.POINTER(ctypes.c_void_p),
                ctypes.POINTER(ctypes.c_void_p)
            ]
            lib.create_test_array_as_pointers.restype = ctypes.c_int
            
            lib.roundtrip_array_pointers.argtypes = [
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_void_p),
                ctypes.POINTER(ctypes.c_void_p)
            ]
            lib.roundtrip_array_pointers.restype = ctypes.c_int
            
            lib.verify_array_size_from_pointers.argtypes = [
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.c_size_t
            ]
            lib.verify_array_size_from_pointers.restype = ctypes.c_int
            
            return lib
        else:
            raise FileNotFoundError(
                f"TEST_POLARS_HELPER_LIB_PATH points to non-existent file: {env_path}"
            )

    raise FileNotFoundError(
        "Could not find test_polars_helper library. "
        "Set TEST_POLARS_HELPER_LIB_PATH environment variable or build the project first."
    )


def pointer_to_arrow_capsule(schema_ptr, array_ptr):
    """
    Convert C pointers to Arrow-compatible PyCapsules.
    
    PyArrow is very particular about how capsules are structured.
    We use ctypes to call PyArrow's C API directly with our pointers.
    """
    # Import the pointers directly using PyArrow's C Data Interface
    # by creating a temporary Python object that exposes __arrow_c_array__
    
    class ArrowCArrayHolder:
        def __init__(self, schema_ptr, array_ptr):
            self.schema_ptr = schema_ptr
            self.array_ptr = array_ptr
        
        def __arrow_c_array__(self, requested_schema=None):  # noqa: ARG001
            """Return schema and array capsules."""
            # Note: requested_schema is part of the Arrow C Data Interface protocol
            from ctypes import pythonapi, py_object, c_void_p, c_char_p
            
            # PyCapsule_New(void *pointer, const char *name, PyCapsule_Destructor destructor)
            pythonapi.PyCapsule_New.restype = py_object
            pythonapi.PyCapsule_New.argtypes = [c_void_p, c_char_p, c_void_p]
            
            schema_capsule = pythonapi.PyCapsule_New(
                self.schema_ptr,
                b"arrow_schema",
                None
            )
            
            array_capsule = pythonapi.PyCapsule_New(
                self.array_ptr,
                b"arrow_array",
                None
            )
            
            return (schema_capsule, array_capsule)
    
    holder = ArrowCArrayHolder(schema_ptr, array_ptr)
    return holder.__arrow_c_array__()


def capsule_to_pointer(capsule, name):
    """Extract the C pointer from a PyCapsule."""
    from ctypes import pythonapi, py_object, c_void_p, c_char_p
    
    # void* PyCapsule_GetPointer(PyObject *capsule, const char *name)
    pythonapi.PyCapsule_GetPointer.restype = c_void_p
    pythonapi.PyCapsule_GetPointer.argtypes = [py_object, c_char_p]
    
    name_bytes = name.encode('utf-8') if name else None
    ptr = pythonapi.PyCapsule_GetPointer(capsule, name_bytes)
    return ptr


def test_create_array_in_cpp():
    """Test creating an array in C++ and importing to Python/Polars."""
    print("\n" + "=" * 70)
    print("Test 1: C++ → Python (Create array in C++, import to Polars)")
    print("=" * 70)
    
    try:
        # Load the C++ helper library
        print("\n1. Loading C++ helper library...")
        lib = load_test_helper_library()
        
        # Create test array in C++ (get raw pointers)
        print("\n2. Creating test array in C++ (sparrow)...")
        schema_ptr = ctypes.c_void_p()
        array_ptr = ctypes.c_void_p()
        
        result = lib.create_test_array_as_pointers(
            ctypes.byref(schema_ptr),
            ctypes.byref(array_ptr)
        )
        
        if result != 0:
            print("   ✗ Failed to create array in C++")
            return False
        
        if not schema_ptr.value or not array_ptr.value:
            print("   ✗ Received null pointers from C++")
            return False
        
        print(f"   ✓ Array created (schema_ptr={hex(schema_ptr.value)}, array_ptr={hex(array_ptr.value)})")
        
        print("\n3. Converting C pointers to PyCapsules in Python...")
        schema_capsule, array_capsule = pointer_to_arrow_capsule(schema_ptr.value, array_ptr.value)
        print("   ✓ PyCapsules created in Python")
        
        print("\n4. Importing to PyArrow...")
        arrow_array = pa.Array._import_from_c_capsule(schema_capsule, array_capsule)
        print(f"   ✓ Arrow type: {arrow_array.type}")
        print(f"   ✓ Arrow values: {arrow_array.to_pylist()}")
        
        # Convert to Polars
        print("\n5. Converting to Polars...")
        polars_series = pl.from_arrow(arrow_array)
        print(f"   ✓ Polars series: {polars_series.to_list()}")
        
        # Verify expected values
        expected = [10, 20, None, 40, 50]
        actual = polars_series.to_list()
        
        if expected == actual:
            print("   ✓ Data matches expected values!")
            print("\n" + "=" * 70)
            print("✓ Test 1 PASSED")
            print("=" * 70)
            return True
        else:
            print(f"   ✗ Data mismatch!")
            print(f"   Expected: {expected}")
            print(f"   Actual: {actual}")
            return False
            
    except Exception as e:
        print(f"\n✗ Test 1 FAILED with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_polars_to_cpp():
    """Test exporting Polars data to C++."""
    print("\n" + "=" * 70)
    print("Test 2: Python → C++ (Export Polars to C++)")
    print("=" * 70)
    
    try:
        lib = load_test_helper_library()
        
        # Create a Polars series
        print("\n1. Creating Polars series...")
        test_series = pl.Series([100, 200, None, 400, 500], dtype=pl.Int32)
        print(f"   Polars series: {test_series.to_list()}")
        
        # Export to Arrow and then to capsules
        print("\n2. Exporting to Arrow C Data Interface...")
        arrow_array = test_series.to_arrow()
        schema_capsule, array_capsule = arrow_array.__arrow_c_array__()
        print("   ✓ Capsules created")
        
        # Extract pointers from capsules
        print("\n3. Extracting raw pointers from capsules...")
        schema_ptr = capsule_to_pointer(schema_capsule, "arrow_schema")
        array_ptr = capsule_to_pointer(array_capsule, "arrow_array")
        print(f"   ✓ Pointers extracted (schema={hex(schema_ptr)}, array={hex(array_ptr)})")
        
        # Verify in C++
        print("\n4. Verifying in C++ (sparrow)...")
        result = lib.verify_array_size_from_pointers(schema_ptr, array_ptr, 5)
        
        if result == 0:
            print("   ✓ C++ successfully imported and verified the array!")
            print("\n" + "=" * 70)
            print("✓ Test 2 PASSED")
            print("=" * 70)
            return True
        else:
            print("   ✗ C++ verification failed")
            return False
            
    except Exception as e:
        print(f"\n✗ Test 2 FAILED with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_roundtrip():
    """Test round-trip: Python → C++ → Python."""
    print("\n" + "=" * 70)
    print("Test 3: Round-trip (Python → C++ → Python)")
    print("=" * 70)
    
    try:
        lib = load_test_helper_library()
        
        # Create a Polars series
        print("\n1. Creating Polars series...")
        original_series = pl.Series([1, 2, None, 4, 5], dtype=pl.Int32)
        print(f"   Original: {original_series.to_list()}")
        
        # Export to capsules
        print("\n2. Exporting to Arrow C Data Interface...")
        arrow_array = original_series.to_arrow()
        schema_capsule_in, array_capsule_in = arrow_array.__arrow_c_array__()
        
        # Extract pointers
        schema_ptr_in = capsule_to_pointer(schema_capsule_in, "arrow_schema")
        array_ptr_in = capsule_to_pointer(array_capsule_in, "arrow_array")
        
        # Round-trip through C++
        print("\n3. Round-tripping through C++...")
        schema_ptr_out = ctypes.c_void_p()
        array_ptr_out = ctypes.c_void_p()
        
        result = lib.roundtrip_array_pointers(
            schema_ptr_in,
            array_ptr_in,
            ctypes.byref(schema_ptr_out),
            ctypes.byref(array_ptr_out)
        )
        
        if result != 0:
            print("   ✗ Round-trip failed in C++")
            return False
        
        if not schema_ptr_out.value or not array_ptr_out.value:
            print("   ✗ Received null output pointers from C++")
            return False
        
        print("   ✓ C++ processed the array")
        
        print("\n4. Converting output to capsules...")
        schema_capsule_out, array_capsule_out = pointer_to_arrow_capsule(schema_ptr_out.value, array_ptr_out.value)
        
        print("\n5. Importing back to Python...")
        arrow_array_out = pa.Array._import_from_c_capsule(schema_capsule_out, array_capsule_out)
        result_series = pl.from_arrow(arrow_array_out)
        print(f"   Result: {result_series.to_list()}")
        
        if original_series.to_list() == result_series.to_list():
            print("   ✓ Round-trip successful - data matches!")
            print("\n" + "=" * 70)
            print("✓ Test 3 PASSED")
            print("=" * 70)
            return True
        else:
            print("   ✗ Data mismatch!")
            print(f"   Original: {original_series.to_list()}")
            print(f"   Result:   {result_series.to_list()}")
            return False
            
    except Exception as e:
        print(f"\n✗ Test 3 FAILED with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all integration tests."""
    print("\n")
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 68 + "║")
    print("║" + "Sparrow-PyCapsule ↔ Polars Integration Tests".center(68) + "║")
    print("║" + "(Pointer-based approach - no PyCapsule_New in C++)".center(68) + "║")
    print("║" + " " * 68 + "║")
    print("╚" + "=" * 68 + "╝")
    
    results = []
    
    # Test 1: C++ → Python
    results.append(("Test 1: C++ → Python", test_create_array_in_cpp()))
    
    # Test 2: Python → C++
    results.append(("Test 2: Python → C++", test_polars_to_cpp()))
    
    # Test 3: Round-trip
    results.append(("Test 3: Round-trip", test_roundtrip()))
    
    # Summary
    print("\n")
    print("=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    
    all_passed = True
    for name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{name}: {status}")
        if not passed:
            all_passed = False
    
    print("=" * 70)
    
    if all_passed:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print("\n❌ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
