#pragma once

#define PY_SSIZE_T_CLEAN
#include <utility>

#include <Python.h>
#include <sparrow-rockfinch/config/config.hpp>

// Forward declarations to avoid including heavy headers
namespace sparrow
{
    class array;
    class arrow_array_stream_proxy;
}

namespace sparrow::rockfinch
{
    /**
     * @brief Imports a sparrow array from schema and array PyCapsules.
     *
     * Transfers ownership from the capsules to the returned array.
     * After successful import, the capsules' release callbacks are set to nullptr,
     * and the returned array owns the data.
     *
     * @param schema_capsule PyCapsule containing an ArrowSchema
     * @param array_capsule PyCapsule containing an ArrowArray
     * @return A sparrow array constructed from the capsules
     * @throws std::invalid_argument if the capsule has the wrong name or was already consumed
     */
    SPARROW_ROCKFINCH_API array import_array_from_capsules(PyObject* schema_capsule, PyObject* array_capsule);

    /**
     * @brief Exports a sparrow array to both schema and array PyCapsules.
     *
     * This is the recommended way to export an array, as it creates both
     * required capsules in one call. The array is moved from and becomes invalid.
     *
     * @param arr The sparrow array to export (will be moved from)
     * @return A pair of (schema_capsule, array_capsule), or (nullptr, nullptr) on error
     */
    SPARROW_ROCKFINCH_API std::pair<PyObject*, PyObject*> export_array_to_capsules(array& arr);

    /**
     * @brief Exports a zero-copy view of a sparrow array to both PyCapsules.
     *
     * The exported ArrowArray tree borrows the buffers of @p arr. @p owner is
     * INCREF'd and DECREF'd (under the GIL) when the last exported node is released.
     *
     * @param arr The sparrow array to borrow buffers from
     * @param owner The Python object keeping the buffers alive
     * @return A pair of (schema_capsule, array_capsule), or (nullptr, nullptr) with a
     *         Python error set on failure
     */
    SPARROW_ROCKFINCH_API std::pair<PyObject*, PyObject*> export_array_view_to_capsules(
        const array& arr,
        PyObject* owner
    );

    // ========================================================================
    // ArrowSchema Export (PyCapsule Interface: __arrow_c_schema__)
    // ========================================================================

    /**
     * @brief Exports the schema of a sparrow array to a PyCapsule.
     *
     * Implements the ArrowSchemaExportable protocol (__arrow_c_schema__).
     * The capsule has the name "arrow_schema" as per the Arrow PyCapsule Interface.
     *
     * @param arr The sparrow array whose schema to export
     * @return A PyCapsule containing an ArrowSchema, or nullptr on error
     */
    SPARROW_ROCKFINCH_API PyObject* export_schema_to_capsule(const array& arr);

    // ========================================================================
    // ArrowArrayStream Export/Import (PyCapsule Interface: __arrow_c_stream__)
    // ========================================================================

    /**
     * @brief Exports an arrow_array_stream_proxy as an ArrowArrayStream PyCapsule.
     *
     * Exports the stream proxy's internal stream to a PyCapsule.
     *
     * @param proxy The stream proxy to export
     * @return A PyCapsule containing an ArrowArrayStream, or nullptr on error
     */
    SPARROW_ROCKFINCH_API PyObject* export_stream_proxy_to_capsule(arrow_array_stream_proxy& proxy);

    /**
     * @brief Imports an arrow_array_stream_proxy from an ArrowArrayStream PyCapsule.
     *
     * Creates a stream proxy from the capsule's stream. The proxy takes ownership
     * of the stream data.
     *
     * @param stream_capsule PyCapsule containing an ArrowArrayStream
     * @return An arrow_array_stream_proxy
     * @throws std::invalid_argument if the capsule has the wrong name or was already consumed
     */
    SPARROW_ROCKFINCH_API arrow_array_stream_proxy import_stream_proxy_from_capsule(
        PyObject* stream_capsule
    );

}
