"""
sparrow_ndarray_interop — Pure-Python ndarray views from Arrow PyCapsule objects.

This module provides functions that create NumPy ndarray views (zero-copy)
or copies from any Python object implementing the ``__arrow_c_array__``
protocol (ArrowArrayExportable).  It works entirely in Python using :py:mod:`ctypes`
to read the Arrow C Data Interface structs out of the PyCapsules — no C++
extension is required.

Usage
-----
>>> import pyarrow as pa
>>> from sparrow_ndarray_interop import ndarray_view, ndarray_copy
>>>
>>> pa_array = pa.array([1, 2, 3], type=pa.int32())
>>> view = ndarray_view(pa_array)
>>> view  # array([1, 2, 3], dtype=int32)
>>>
>>> copy = ndarray_copy(pa_array)
>>> copy[0] = 99  # safe, owns its data
"""

from __future__ import annotations

import ctypes
import weakref

import numpy as np

# =============================================================================
# Capsule lifetime registry
# =============================================================================
# NumPy 2.x ndarrays do not support arbitrary attribute assignment, so we
# use weakref.finalize to tie the Arrow PyCapsule lifetime to the ndarray.

_CAPSULE_REGISTRY: dict[int, tuple] = {}


def _register_capsules(arr: np.ndarray, schema_capsule: object, array_capsule: object) -> None:
    """Keep *schema_capsule* and *array_capsule* alive as long as *arr* exists."""
    key = id(arr)
    _CAPSULE_REGISTRY[key] = (schema_capsule, array_capsule)
    weakref.finalize(arr, _CAPSULE_REGISTRY.pop, key, None)

# =============================================================================
# Arrow format → NumPy dtype lookup
# See: https://arrow.apache.org/docs/format/CDataInterface.html
# =============================================================================

_ARROW_FORMAT_TO_DTYPE: dict[str, np.dtype] = {
    "c": np.dtype(np.int8),
    "C": np.dtype(np.uint8),
    "s": np.dtype(np.int16),
    "S": np.dtype(np.uint16),
    "i": np.dtype(np.int32),
    "I": np.dtype(np.uint32),
    "l": np.dtype(np.int64),
    "L": np.dtype(np.uint64),
    "e": np.dtype(np.float16),
    "f": np.dtype(np.float32),
    "g": np.dtype(np.float64),
    "d": np.dtype(np.float64),   # legacy alias used by some Arrow implementations
    "b": np.dtype(np.bool_),
}

# =============================================================================
# Arrow C Data Interface struct definitions
# =============================================================================


class _ArrowSchema(ctypes.Structure):
    _fields_ = [
        ("format", ctypes.c_char_p),
        ("name", ctypes.c_char_p),
        ("metadata", ctypes.c_void_p),
        ("flags", ctypes.c_int64),
        ("n_children", ctypes.c_int64),
        ("children", ctypes.c_void_p),
        ("dictionary", ctypes.c_void_p),
        ("release", ctypes.c_void_p),
        ("private_data", ctypes.c_void_p),
    ]


class _ArrowArray(ctypes.Structure):
    _fields_ = [
        ("length", ctypes.c_int64),
        ("null_count", ctypes.c_int64),
        ("offset", ctypes.c_int64),
        ("n_buffers", ctypes.c_int64),
        ("n_children", ctypes.c_int64),
        ("buffers", ctypes.c_void_p),
        ("children", ctypes.c_void_p),
        ("dictionary", ctypes.c_void_p),
        ("release", ctypes.c_void_p),
        ("private_data", ctypes.c_void_p),
    ]


# =============================================================================
# Internal helpers
# =============================================================================


def _get_capsule_pointer(capsule: object, name: str) -> int:
    """Extract the raw C pointer from a named PyCapsule.

    Parameters
    ----------
    capsule : object
        A PyCapsule returned by ``__arrow_c_array__()``.
    name : str
        Expected capsule name (``"arrow_schema"`` or ``"arrow_array"``).

    Returns
    -------
    int
        The raw pointer value stored in the capsule.

    Raises
    ------
    ValueError
        If the capsule does not have the expected name.
    """
    ctypes.pythonapi.PyCapsule_GetPointer.restype = ctypes.c_void_p
    ctypes.pythonapi.PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    ptr = ctypes.pythonapi.PyCapsule_GetPointer(capsule, name.encode())
    if ptr is None:
        raise ValueError(f"PyCapsule is not a valid '{name}' capsule")
    return ptr


def _read_arrow_schema(schema_capsule: object) -> _ArrowSchema:
    """Read the ArrowSchema struct from a schema PyCapsule."""
    ptr = _get_capsule_pointer(schema_capsule, "arrow_schema")
    return _ArrowSchema.from_address(ptr)


def _read_arrow_array(array_capsule: object) -> _ArrowArray:
    """Read the ArrowArray struct from an array PyCapsule."""
    ptr = _get_capsule_pointer(array_capsule, "arrow_array")
    return _ArrowArray.from_address(ptr)


def _resolve_dtype(format_str: str) -> np.dtype:
    """Convert an Arrow format string to a NumPy dtype.

    Raises
    ------
    TypeError
        If the format is not a supported primitive numeric type.
    """
    dtype = _ARROW_FORMAT_TO_DTYPE.get(format_str)
    if dtype is None:
        raise TypeError(
            f"Unsupported Arrow format '{format_str}'. "
            f"Supported formats: {sorted(_ARROW_FORMAT_TO_DTYPE.keys())}"
        )
    if dtype == np.dtype(np.bool_):
        raise TypeError(
            "ndarray_view does not support bool arrays "
            "because Arrow stores bool values bit-packed. "
            "Use ndarray_copy() or to_numpy() instead."
        )
    return dtype


# =============================================================================
# Public API
# =============================================================================


def ndarray_view(obj: object) -> np.ndarray:
    """Create a **zero-copy, read-only** ndarray view from an Arrow-exportable object.

    Calls ``obj.__arrow_c_array__()`` to obtain the Arrow C Data Interface
    structs, then maps the Arrow format to a NumPy dtype and creates a
    ``np.frombuffer`` view directly over the Arrow data buffer.

    The returned array is read-only (``arr.flags.writeable == False``) and
    does **not** own its data (``arr.flags.owndata == False``).  The lifetime
    of the underlying Arrow memory is tied to the PyCapsules, which are kept
    alive by storing them on the returned ndarray as
    ``arr._arrow_capsules``.

    Parameters
    ----------
    obj : object
        Any object implementing ``__arrow_c_array__()`` (ArrowArrayExportable
        protocol).  Examples include ``sparrow_rockfinch.SparrowArray``,
        ``pyarrow.Array``, and Polars Series.

    Returns
    -------
    np.ndarray
        A 1D read-only ndarray backed by the Arrow buffer.

    Raises
    ------
    TypeError
        If the object does not implement ``__arrow_c_array__``, the Arrow
        format is unsupported, or the array is nullable.

    Examples
    --------
    >>> import pyarrow as pa
    >>> arr = pa.array([1, 2, 3], type=pa.int32())
    >>> view = ndarray_view(arr)
    >>> view  # array([1, 2, 3], dtype=int32)
    >>> view.flags.writeable
    False
    """
    try:
        capsules = obj.__arrow_c_array__()
    except AttributeError:
        raise TypeError(
            "Object does not implement __arrow_c_array__ "
            "(ArrowArrayExportable protocol)"
        ) from None

    if not isinstance(capsules, tuple) or len(capsules) != 2:
        raise TypeError("__arrow_c_array__ must return a 2-tuple of PyCapsules")

    schema_capsule, array_capsule = capsules
    schema = _read_arrow_schema(schema_capsule)
    arr_struct = _read_arrow_array(array_capsule)

    # Resolve dtype from Arrow format string
    format_str = schema.format.decode() if schema.format else ""
    dtype = _resolve_dtype(format_str)

    length = arr_struct.length
    offset = arr_struct.offset

    if arr_struct.null_count != 0:
        raise TypeError(
            f"ndarray_view does not support nullable arrays "
            f"(null_count={arr_struct.null_count})."
        )

    if arr_struct.n_buffers < 2:
        raise TypeError(
            f"Expected at least 2 buffers for a primitive array, "
            f"got n_buffers={arr_struct.n_buffers}."
        )

    # Read the buffer pointers: buffers[0] = validity, buffers[1] = data
    BufferPtrArray = ctypes.c_void_p * arr_struct.n_buffers
    buffer_ptrs = BufferPtrArray.from_address(arr_struct.buffers)
    data_start = buffer_ptrs[1] + offset * dtype.itemsize
    nbytes = (length - offset) * dtype.itemsize

    # Create a ctypes buffer over the Arrow data buffer.
    # np.frombuffer keeps the buffer alive via its .base reference.
    ctypes_buffer = (ctypes.c_char * nbytes).from_address(data_start)
    # np.frombuffer creates a writable view over the ctypes buffer.
    # We must explicitly mark it read-only to enforce the zero-copy contract.
    arr = np.frombuffer(ctypes_buffer, dtype=dtype, count=length - offset)
    arr.flags.writeable = False

    # Tie the Arrow capsule lifetime to the returned ndarray.
    _register_capsules(arr, schema_capsule, array_capsule)

    return arr


def ndarray_copy(obj: object) -> np.ndarray:
    """Create a **writable, owning** ndarray copy from an Arrow-exportable object.

    Equivalent to calling :func:`ndarray_view` followed by ``.copy()``, which
    allocates a fresh buffer and copies the data.  The returned array is
    independent of the source Arrow memory.

    Parameters
    ----------
    obj : object
        See :func:`ndarray_view`.

    Returns
    -------
    np.ndarray
        A writable 1D ndarray that owns its data.

    Examples
    --------
    >>> import pyarrow as pa
    >>> arr = pa.array([1, 2, 3], type=pa.int32())
    >>> copy = ndarray_copy(arr)
    >>> copy.flags.writeable
    True
    >>> copy.flags.owndata
    True
    """
    view = ndarray_view(obj)
    return np.array(view, copy=True)  # explicit copy — independent, writable
