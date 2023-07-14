string(APPEND CMAKE_C_FLAGS_DEBUG " -Werror=format-security")
string(APPEND CMAKE_C_FLAGS_RELEASE " -Werror=format-security")
if(USE_ADDR_SANITIZER)
  string(APPEND CMAKE_C_FLAGS_DEBUG " -fsanitize=address")
  string(APPEND CMAKE_C_FLAGS_RELEASE " -fsanitize=address")
endif()

# Set the template VOL connector name
set(PARH5_VOL_LIB parallax_vol_connector)

set(PARH5_VOL_C_SOURCE_FILES
    parallax_vol_connector.c
    parallax_vol_file.c
    parallax_vol_group.c
    parallax_vol_object.c
    parallax_vol_introspect.c
    parallax_vol_dataset.c
    parallax_vol_inode.c
    parallax_vol_attribute.c
    parallax_vol_links.c
    parallax_vol_metrics.c
    parallax_vol_tile_cache.c)

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  set_source_files_properties(PARH5_VOL_C_SOURCE_FILES
                              PROPERTIES COMPILE_FLAGS "${CMAKE_C_FLAGS_DEBUG}")
else()
  set_source_files_properties(
    PARH5_VOL_C_SOURCE_FILES PROPERTIES COMPILE_FLAGS
                                        "${CMAKE_C_FLAGS_RELEASE}")
endif()

# Build the Parallax VOL plugin
add_library(
  ${PARH5_VOL_LIB} SHARED
  parallax_vol_connector.c
  parallax_vol_file.c
  parallax_vol_group.c
  parallax_vol_object.c
  parallax_vol_introspect.c
  parallax_vol_dataset.c
  parallax_vol_inode.c
  parallax_vol_attribute.c
  parallax_vol_links.c
  parallax_vol_metrics.c
  parallax_vol_tile_cache.c)

target_link_libraries(${PARH5_VOL_LIB} log parallax)
if(USE_ADDR_SANITIZER)
  target_link_libraries(${PARH5_VOL_LIB} log parallax asan)
else() # Conditionally define DISABLE_LOGGING for the library
  target_compile_definitions(${PARH5_VOL_LIB}
                             PRIVATE $<$<CONFIG:Release>:PARH5_DISABLE_LOGGING>)
  target_link_libraries(${PARH5_VOL_LIB} log parallax)
endif()

target_compile_options(
  ${PARH5_VOL_LIB}
  PRIVATE -Wstrict-prototypes
          -Wall
          -Wextra
          -fno-omit-frame-pointer
          -Wunused
          -Wuninitialized
          -Wimplicit-function-declaration
          -Wshadow
          -pedantic
          -pedantic-errors
          -Wmisleading-indentation
          -pipe)

set_target_properties(${PARH5_VOL_LIB} PROPERTIES VERSION ${PROJECT_VERSION})
set_target_properties(${PARH5_VOL_LIB} PROPERTIES SOVERSION 1)
set_target_properties(${PARH5_VOL_LIB} PROPERTIES PUBLIC_HEADER
                                                  "${PARH5_VOL_LIB}.h")
