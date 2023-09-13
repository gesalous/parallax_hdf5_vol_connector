# Build the test
add_executable(vol_plugin vol_plugin.c)
target_include_directories(vol_plugin PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(vol_plugin ${HDF5_C_LIBRARIES})

add_executable(test_parallax_plugin test_parallax_plugin.c)
target_include_directories(test_parallax_plugin
                           PRIVATE "${project_source_dir}/src")
target_link_libraries(test_parallax_plugin log ${HDF5_C_LIBRARIES})

add_executable(test_dirs test_dirs.c)
target_include_directories(test_dirs PRIVATE "${project_source_dir}/src")
target_link_libraries(test_dirs log ${HDF5_C_LIBRARIES})

add_executable(test_multi_dimensions test_multi_dimensions.c)
target_include_directories(test_multi_dimensions
                           PRIVATE "${project_source_dir}/src")
target_link_libraries(test_multi_dimensions log ${HDF5_C_LIBRARIES})

add_executable(test_partial_updates test_partial_updates.c)
target_include_directories(test_partial_updates
                           PRIVATE "${project_source_dir}/src")
target_link_libraries(test_partial_updates log ${HDF5_C_LIBRARIES})

add_executable(test_different_dimensions test_different_dimensions.c)
target_include_directories(test_different_dimensions
                           PRIVATE "${project_source_dir}/src")
target_link_libraries(test_different_dimensions log ${HDF5_C_LIBRARIES})
target_compile_options(test_different_dimensions PRIVATE -g)
# target_compile_options(test_different_dimensions PRIVATE -O1)
target_link_options(test_different_dimensions PRIVATE -rdynamic)

add_executable(test_random_updates test_random_updates.c)
target_include_directories(test_random_updates
                           PRIVATE "${project_source_dir}/src")
target_link_libraries(test_random_updates log ${HDF5_C_LIBRARIES})
target_compile_options(test_random_updates PRIVATE -g)
target_link_options(test_random_updates PRIVATE -rdynamic)

# Add the test
add_test(vol_plugin vol_plugin)
set_tests_properties(
  vol_plugin PROPERTIES ENVIRONMENT
                        "HDF5_PLUGIN_PATH=${PROJECT_BINARY_DIR}/src")

# Add the test
add_test(test_parallax_plugin test_parallax_plugin)
set_tests_properties(
  test_parallax_plugin PROPERTIES ENVIRONMENT
                                  "HDF5_PLUGIN_PATH=${PROJECT_BINARY_DIR}/src")

# Add the test
add_test(test_dirs test_dirs)
set_tests_properties(
  test_dirs PROPERTIES ENVIRONMENT "HDF5_PLUGIN_PATH=${PROJECT_BINARY_DIR}/src")

# Add the test
add_test(test_multi_dimensions test_multi_dimensions)
set_tests_properties(
  test_multi_dimensions PROPERTIES ENVIRONMENT
                                   "HDF5_PLUGIN_PATH=${PROJECT_BINARY_DIR}/src")
# Add the test
add_test(test_partial_updates test_partial_updates)
set_tests_properties(
  test_partial_updates PROPERTIES ENVIRONMENT
                                  "HDF5_PLUGIN_PATH=${PROJECT_BINARY_DIR}/src")

# Add the test
add_test(test_different_dimensions test_different_dimensions)
set_tests_properties(
  test_different_dimensions
  PROPERTIES ENVIRONMENT "HDF5_PLUGIN_PATH=${PROJECT_BINARY_DIR}/src")

# Add the test
add_test(test_random_updates test_random_updates)
set_tests_properties(
  test_random_updates PROPERTIES ENVIRONMENT
                                 "HDF5_PLUGIN_PATH=${PROJECT_BINARY_DIR}/src")

# Add custom library and include paths to CMAKE_PREFIX_PATH or use -D options
list(APPEND CMAKE_PREFIX_PATH "${custom_library_path}" "${custom_include_path}")

# Find the library
find_library(LIBRARY_NAME tiledb)

# Check if the library was found
if(NOT LIBRARY_NAME)
  message(
    WARNING "tiledb library not found. Cannot build tiledb test ok no worries.")
else()
  add_executable(test_tiledb test_tiledb.c)
  # Split the custom_include_path string into a list
  string(REPLACE ";" ";" custom_include_path_list "${custom_include_path}")

  target_include_directories(test_tiledb PRIVATE "${project_source_dir}/src"
                                                 "${custom_include_path_list}")
  target_link_libraries(test_tiledb log ${LIBRARY_NAME})
endif()
