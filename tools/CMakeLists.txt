add_executable(parallax_server cp_to_parallax.c parallax_server.c)
target_include_directories(parallax_server PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(parallax_server log ${HDF5_C_LIBRARIES})

target_compile_options(
  parallax_server
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

add_executable(parallax_client parallax_client.c)
target_include_directories(parallax_client PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(parallax_client log ${HDF5_C_LIBRARIES})

target_compile_options(
  parallax_client
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
