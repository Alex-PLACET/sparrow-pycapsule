#include <sparrow-rockfinch/pycapsule.hpp>

#include <atomic>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <sparrow/array.hpp>
#include <sparrow/c_interface.hpp>
#include <sparrow/arrow_interface/arrow_array_stream_proxy.hpp>
#include <sparrow/arrow_interface/arrow_schema.hpp>
#include <sparrow/arrow_interface/arrow_array.hpp>
#include <sparrow/arrow_interface/arrow_array_stream.hpp>
#include <sparrow/arrow_interface/arrow_array_stream/private_data.hpp>

namespace sparrow::rockfinch
{
    namespace
    {
        // Internal capsule name constants
        constexpr const char* arrow_schema_str = "arrow_schema";
        constexpr const char* arrow_array_str = "arrow_array";
        constexpr const char* arrow_array_stream_str = "arrow_array_stream";

        template <class T>
        void release_arrow_capsule(PyObject* capsule)
        {
            auto* ptr = static_cast<T*>(PyCapsule_GetPointer(capsule, PyCapsule_GetName(capsule)));
            if (ptr == nullptr)
            {
                return;
            }
            if (ptr->release != nullptr)
            {
                ptr->release(ptr);
            }
            delete ptr;
        }

        // Takes ownership of ptr. On failure releases it and returns nullptr (Python error set).
        template <class T>
        PyObject* make_arrow_capsule(T* ptr, const char* name)
        {
            PyObject* capsule = PyCapsule_New(ptr, name, release_arrow_capsule<T>);
            if (capsule == nullptr)
            {
                if (ptr->release != nullptr)
                {
                    ptr->release(ptr);
                }
                delete ptr;
            }
            return capsule;
        }

        template <class T>
        T* get_unconsumed(PyObject* capsule, const char* name, const char* what)
        {
            auto* ptr = static_cast<T*>(PyCapsule_GetPointer(capsule, name));
            if (ptr == nullptr)
            {
                PyErr_Clear();
                throw std::invalid_argument(std::string("Expected a PyCapsule named '") + name + "'");
            }
            if (ptr->release == nullptr)
            {
                throw std::invalid_argument(std::string(what) + " PyCapsule has already been consumed");
            }
            return ptr;
        }

        struct borrowed_owner
        {
            PyObject* owner;
            std::atomic<std::size_t> refs{0};
        };

        struct borrowed_node
        {
            borrowed_owner* shared;
            std::vector<ArrowArray*> children;
            ArrowArray* dictionary = nullptr;
        };

        void drop_borrowed_owner(borrowed_owner* shared)
        {
            if (shared->refs.fetch_sub(1, std::memory_order_acq_rel) != 1)
            {
                return;
            }
            if (Py_IsInitialized() != 0)
            {
                const PyGILState_STATE gil = PyGILState_Ensure();
                Py_DECREF(shared->owner);
                PyGILState_Release(gil);
            }
            else if (PyGILState_Check() != 0)
            {
                // Interpreter is finalizing: PyGILState_Ensure is unsafe here, but this
                // thread already holds the GIL, so a plain DECREF is valid.
                Py_DECREF(shared->owner);
            }
            // ponytail: owner leaks if the consumer releases on a non-GIL thread after interpreter shutdown
            delete shared;
        }

        void release_borrowed_array(ArrowArray* array);

        // Shallow-copies src into dst (buffers borrowed); every node holds one ref on shared.
        void wrap_borrowed_array(const ArrowArray& src, ArrowArray& dst, borrowed_owner* shared)
        {
            dst = src;
            auto* node = new borrowed_node{shared, {}, nullptr};
            shared->refs.fetch_add(1, std::memory_order_relaxed);
            node->children.resize(static_cast<std::size_t>(src.n_children));
            for (std::size_t i = 0; i < node->children.size(); ++i)
            {
                node->children[i] = new ArrowArray{};
                wrap_borrowed_array(*src.children[i], *node->children[i], shared);
            }
            dst.children = node->children.empty() ? nullptr : node->children.data();
            if (src.dictionary != nullptr)
            {
                node->dictionary = new ArrowArray{};
                wrap_borrowed_array(*src.dictionary, *node->dictionary, shared);
            }
            dst.dictionary = node->dictionary;
            dst.private_data = node;
            dst.release = &release_borrowed_array;
        }

        void release_borrowed_array(ArrowArray* array)
        {
            auto* node = static_cast<borrowed_node*>(array->private_data);
            for (ArrowArray* child : node->children)
            {
                if (child->release != nullptr)
                {
                    child->release(child);  // null if consumer moved it out
                }
                delete child;
            }
            if (node->dictionary != nullptr)
            {
                if (node->dictionary->release != nullptr)
                {
                    node->dictionary->release(node->dictionary);
                }
                delete node->dictionary;
            }
            borrowed_owner* shared = node->shared;
            delete node;
            array->release = nullptr;
            drop_borrowed_owner(shared);
        }
    }

    array import_array_from_capsules(PyObject* schema_capsule, PyObject* array_capsule)
    {
        // Validate both capsules before mutating either so a failure leaves them untouched.
        ArrowSchema* schema = get_unconsumed<ArrowSchema>(schema_capsule, arrow_schema_str, "ArrowSchema");
        ArrowArray* arr = get_unconsumed<ArrowArray>(array_capsule, arrow_array_str, "ArrowArray");

        // Move the data from the capsule structures
        ArrowSchema schema_moved = *schema;
        ArrowArray array_moved = *arr;

        // Mark as released to prevent the capsule destructors from freeing the data
        schema->release = nullptr;
        arr->release = nullptr;

        return array(std::move(array_moved), std::move(schema_moved));
    }

    std::pair<PyObject*, PyObject*> export_array_to_capsules(array& arr)
    {
        // Extract both schema and array from the sparrow array (moves ownership)
        auto [arrow_array, arrow_schema] = sparrow::extract_arrow_structures(std::move(arr));

        PyObject* schema_capsule = make_arrow_capsule(new ArrowSchema(arrow_schema), arrow_schema_str);
        PyObject* array_capsule = make_arrow_capsule(new ArrowArray(arrow_array), arrow_array_str);
        if (schema_capsule == nullptr || array_capsule == nullptr)
        {
            Py_XDECREF(schema_capsule);
            Py_XDECREF(array_capsule);
            return {nullptr, nullptr};
        }
        return {schema_capsule, array_capsule};
    }

    PyObject* export_schema_to_capsule(const array& arr)
    {
        // Get pointer to the schema (does not move ownership)
        const ArrowSchema* schema = sparrow::get_arrow_schema(arr);

        auto* schema_ptr = new ArrowSchema();
        sparrow::copy_schema(*schema, *schema_ptr);
        return make_arrow_capsule(schema_ptr, arrow_schema_str);
    }

    PyObject* export_stream_proxy_to_capsule(arrow_array_stream_proxy& proxy)
    {
        ArrowArrayStream* stream_ptr = proxy.export_stream();
        if (stream_ptr == nullptr)
        {
            PyErr_SetString(PyExc_RuntimeError, "Failed to export stream from proxy");
            return nullptr;
        }

        auto* heap_stream = new ArrowArrayStream(*stream_ptr);
        // Clear the source to prevent double-release
        stream_ptr->release = nullptr;

        return make_arrow_capsule(heap_stream, arrow_array_stream_str);
    }

    std::pair<PyObject*, PyObject*> export_array_view_to_capsules(const array& arr, PyObject* owner)
    {
        auto* shared = new borrowed_owner{owner};
        Py_INCREF(owner);
        auto* array_ptr = new ArrowArray{};
        wrap_borrowed_array(*sparrow::get_arrow_array(arr), *array_ptr, shared);
        auto* schema_ptr = new ArrowSchema{};
        sparrow::copy_schema(*sparrow::get_arrow_schema(arr), *schema_ptr);
        PyObject* schema_capsule = make_arrow_capsule(schema_ptr, arrow_schema_str);
        PyObject* array_capsule = make_arrow_capsule(array_ptr, arrow_array_str);
        if (schema_capsule == nullptr || array_capsule == nullptr)
        {
            Py_XDECREF(schema_capsule);
            Py_XDECREF(array_capsule);
            return {nullptr, nullptr};
        }
        return {schema_capsule, array_capsule};
    }

    arrow_array_stream_proxy import_stream_proxy_from_capsule(PyObject* stream_capsule)
    {
        ArrowArrayStream* stream = get_unconsumed<ArrowArrayStream>(
            stream_capsule,
            arrow_array_stream_str,
            "ArrowArrayStream"
        );

        // Move the stream into a new proxy
        ArrowArrayStream stream_copy = *stream;
        // Mark the capsule's stream as consumed
        stream->release = nullptr;

        return arrow_array_stream_proxy(std::move(stream_copy));
    }
}
