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
