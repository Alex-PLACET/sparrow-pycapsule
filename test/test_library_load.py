#!/usr/bin/env python3
"""
Minimal test to debug library loading issues.
This script tests each step individually to identify where segfaults occur.
"""

import sys
import os
import ctypes
from pathlib import Path

def step(num, description):
    """Print a test step."""
    print(f"\n{'='*60}")
    print(f"Step {num}: {description}")
    print('='*60)

def main():
    print("\n" + "="*60)
    print("Library Loading Debug Test")
    print("="*60)
    
    try:
        # Step 1: Check environment variables
        step(1, "Checking environment variables")
        helper_path = os.environ.get('TEST_POLARS_HELPER_LIB_PATH')
        main_path = os.environ.get('SPARROW_PYCAPSULE_LIB_PATH')
        
        print(f"TEST_POLARS_HELPER_LIB_PATH: {helper_path}")
        print(f"SPARROW_PYCAPSULE_LIB_PATH: {main_path}")
        
        if not helper_path:
            print("ERROR: TEST_POLARS_HELPER_LIB_PATH not set")
            return 1
        
        if not main_path:
            print("ERROR: SPARROW_PYCAPSULE_LIB_PATH not set")
            return 1
        
        # Step 2: Check files exist
        step(2, "Checking library files exist")
        helper_file = Path(helper_path)
        main_file = Path(main_path)
        
        print(f"Helper library exists: {helper_file.exists()} ({helper_file})")
        print(f"Main library exists: {main_file.exists()} ({main_file})")
        
        if not helper_file.exists():
            print(f"ERROR: Helper library not found at {helper_file}")
            return 1
        
        if not main_file.exists():
            print(f"ERROR: Main library not found at {main_file}")
            return 1
        
        # Step 3: Test ctypes import
        step(3, "Testing ctypes module")
        print("ctypes imported successfully")
        print(f"ctypes.CDLL: {ctypes.CDLL}")
        
        # Step 4: Try loading main library
        step(4, "Loading sparrow-pycapsule library")
        try:
            main_lib = ctypes.CDLL(str(main_file))
            print("✓ Main library loaded successfully")
        except Exception as e:
            print(f"✗ Failed to load main library: {e}")
            return 1
        
        # Step 5: Try loading helper library
        step(5, "Loading test_polars_helper library")
        try:
            helper_lib = ctypes.CDLL(str(helper_file))
            print("✓ Helper library loaded successfully")
        except Exception as e:
            print(f"✗ Failed to load helper library: {e}")
            return 1
        
        # Step 6: Check if init_python exists
        step(6, "Checking for init_python function")
        try:
            if hasattr(helper_lib, 'init_python'):
                print("✓ init_python function found")
            else:
                print("✗ init_python function not found")
                print(f"Available attributes: {dir(helper_lib)}")
                return 1
        except Exception as e:
            print(f"✗ Error checking for init_python: {e}")
            return 1
        
        # Step 7: Call init_python
        step(7, "Calling init_python()")
        print("About to call init_python()...")
        sys.stdout.flush()
        
        try:
            helper_lib.init_python()
            print("✓ init_python() called successfully")
        except Exception as e:
            print(f"✗ init_python() failed: {e}")
            return 1
        
        # Step 8: Check Python state
        step(8, "Checking Python interpreter state")
        import sys as sys2
        print(f"Python version: {sys2.version}")
        print(f"Python initialized: {sys2.version_info}")
        
        print("\n" + "="*60)
        print("✓ ALL STEPS COMPLETED SUCCESSFULLY")
        print("="*60)
        return 0
        
    except Exception as e:
        print(f"\n✗ EXCEPTION: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
