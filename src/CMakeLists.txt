set(EXISTING_FLAGS "-fno-omit-frame-pointer -Wall")
set(CMAKE_C_FLAGS "${EXISTING_FLAGS}")
if(USE_ADDR_SANITIZER)
  set(CMAKE_C_FLAGS "${EXISTING_FLAGS} -fsanitize=address")
else()
  set(CMAKE_C_FLAGS "${EXISTING_FLAGS}")
endif()

# Set the template VOL connector name
set(TVC_NAME parallax_vol_connector)

# Build the template VOL
add_library(
  ${TVC_NAME} SHARED
  parallax_vol_connector.c parallax_vol_file.c parallax_vol_group.c
  parallax_vol_object.c parallax_vol_introspect.c parallax_vol_dataset.c)

target_link_libraries(${TVC_NAME} log parallax)
if(USE_ADDR_SANITIZER)
  target_link_libraries(${TVC_NAME} log parallax asan)
else()
  target_link_libraries(${TVC_NAME} log parallax)
endif()

target_compile_options(
  ${TVC_NAME}
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

set_target_properties(${TVC_NAME} PROPERTIES VERSION ${PROJECT_VERSION})
set_target_properties(${TVC_NAME} PROPERTIES SOVERSION 1)
set_target_properties(${TVC_NAME} PROPERTIES PUBLIC_HEADER "${TVC_NAME}.h")
