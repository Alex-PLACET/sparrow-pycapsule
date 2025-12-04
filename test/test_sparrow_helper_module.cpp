#include <cstdint>
#include <vector>

#include <nanobind/nanobind.h>
#include <nanobind/stl/pair.h>

#include <sparrow/array.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/utils/nullable.hpp>

#include <sparrow-pycapsule/pycapsule.hpp>
#include <sparrow-pycapsule/sparrow_array_python_class.hpp>

namespace nb = nanobind;

/**
 * Create a test sparrow array with sample data.
 */
sparrow::array create_test_sparrow_array()
{
    // Create a test array with nullable integers
    std::vector<sparrow::nullable<int32_t>> values = {
        sparrow::make_nullable<int32_t>(10, true),
        sparrow::make_nullable<int32_t>(20, true),
        sparrow::make_nullable<int32_t>(0, false),  // null
        sparrow::make_nullable<int32_t>(40, true),
        sparrow::make_nullable<int32_t>(50, true)
    };

    sparrow::primitive_array<int32_t> prim_array(std::move(values));
    return sparrow::array(std::move(prim_array));
}

/**
 * Create a SparrowArray from an object implementing __arrow_c_array__.
 */
sparrow::pycapsule::SparrowArray create_sparrow_array_from_arrow(nb::object arrow_array)
{
    // Get __arrow_c_array__ method from the input object
    if (!nb::hasattr(arrow_array, "__arrow_c_array__"))
    {
        throw nb::type_error(
            "Input object must implement __arrow_c_array__ (ArrowArrayExportable protocol)"
        );
    }

    // Call __arrow_c_array__() to get the capsules
    nb::object capsules = arrow_array.attr("__arrow_c_array__")();

    // Unpack the tuple (schema_capsule, array_capsule)
    if (!nb::isinstance<nb::tuple>(capsules) || nb::len(capsules) != 2)
    {
        throw nb::type_error("__arrow_c_array__ must return a tuple of 2 elements");
    }

    nb::tuple capsule_tuple = nb::cast<nb::tuple>(capsules);
    PyObject* schema_capsule = capsule_tuple[0].ptr();
    PyObject* array_capsule = capsule_tuple[1].ptr();

    return sparrow::pycapsule::SparrowArray(schema_capsule, array_capsule);
}

NB_MODULE(test_sparrow_helper, m)
{
    m.doc() = "Native Python extension providing SparrowArray type for Arrow data exchange.\n"
              "Higher-level helpers are available in sparrow_helpers.py.";

    // Define the SparrowArray class using nanobind
    nb::class_<sparrow::pycapsule::SparrowArray>(m, "SparrowArray",
        "SparrowArray - Arrow array wrapper implementing __arrow_c_array__.\n\n"
        "This class wraps a sparrow array and implements the Arrow PyCapsule\n"
        "Interface (ArrowArrayExportable protocol), allowing it to be passed\n"
        "directly to libraries like Polars via pl.from_arrow().\n\n"
        "To create a SparrowArray from a PyArrow array, use:\n"
        "    sparrow_array = SparrowArray.from_arrow(pyarrow_array)")
        .def_static("from_arrow", &create_sparrow_array_from_arrow, nb::arg("arrow_array"),
            "Construct a SparrowArray from an Arrow-compatible object.\n\n"
            "Parameters\n"
            "----------\n"
            "arrow_array : ArrowArrayExportable\n"
            "    An object implementing __arrow_c_array__ (e.g., PyArrow array).\n\n"
            "Returns\n"
            "-------\n"
            "SparrowArray\n"
            "    A new SparrowArray wrapping the input data.")
        .def("__arrow_c_array__", [](const sparrow::pycapsule::SparrowArray& self, nb::object /*requested_schema*/) {
            auto [schema, array] = self.export_to_capsules();
            // Create a tuple and return ownership to Python
            nb::object schema_obj = nb::steal(schema);
            nb::object array_obj = nb::steal(array);
            return nb::make_tuple(schema_obj, array_obj);
        }, nb::arg("requested_schema") = nb::none(),
            "Export the array via the Arrow PyCapsule interface.\n\n"
            "Parameters\n"
            "----------\n"
            "requested_schema : object, optional\n"
            "    Requested schema for the output (typically ignored).\n\n"
            "Returns\n"
            "-------\n"
            "tuple[object, object]\n"
            "    A tuple of (schema_capsule, array_capsule).")
        .def("size", &sparrow::pycapsule::SparrowArray::size,
            "Get the number of elements in the array.\n\n"
            "Returns\n"
            "-------\n"
            "int\n"
            "    The size of the array.");

    // Add the test helper function
    m.def("create_test_array", []() {
        sparrow::array arr = create_test_sparrow_array();
        return sparrow::pycapsule::SparrowArray(std::move(arr));
    }, "Create a test array and return a SparrowArray object implementing __arrow_c_array__.");
}
