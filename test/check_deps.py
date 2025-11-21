#!/usr/bin/env python3
"""Check if required Python dependencies are installed."""

import sys

try:
    import polars
    print(f"polars version: {polars.__version__}")
except ImportError:
    print("ERROR: polars not installed")
    sys.exit(1)

try:
    import pyarrow as pa
    print(f"pyarrow version: {pa.__version__}")
except ImportError:
    print("ERROR: pyarrow not installed")
    sys.exit(1)

print("\nAll dependencies installed!")
sys.exit(0)
