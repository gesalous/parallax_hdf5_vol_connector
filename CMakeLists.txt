include(cmake_utils/PreventInSouceBuilds.cmake.in)
cmake_minimum_required(VERSION 3.11.0 FATAL_ERROR)
project (parallax_vol VERSION 1.0.0 DESCRIPTION "parallax VOL connector" LANGUAGES C)
list(APPEND CMAKE_PREFIX_PATH "/home/gesalous/HDF5_staff/hdf5")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Enable testing
enable_testing ()
include (CTest)

# Find HDF5
find_package (HDF5 1.13 COMPONENTS C REQUIRED)
link_directories (${HDF5_LIBRARY_DIRS})
include_directories (${HDF5_INCLUDE_DIR})
set (LINK_LIBS ${LINK_LIBS} ${HDF5_C_${LIB_TYPE}_LIBRARY})

# It's really easy to pick up the wrong HDF5 library if you set the path
# wrong. Turn this on for added confirmation that you got it right.
#message (DEPRECATION "Include: ${HDF5_INCLUDE_DIR}")

add_subdirectory (src)
add_subdirectory (test)

