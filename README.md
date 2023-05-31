# Parallax VOL Connector

This is an HDF5 (https://www.hdfgroup.org/solutions/hdf5/) Virtual Object Layer plugin for Parallax (https://github.com/CARV-ICS-FORTH/parallax) key value store. This plugin enables HDF5 to store multidimensional arrays atop of fast storage devices such as NVMe SSDs with the use of Parallax KV store.


## Getting started

You will need a few things to build the code in this repository:

* HDF5 1.13.0 or later
* CMake (3.9 or later) or the Autotools (autoconf 2.69 or later and matching automake, etc.)

### Autotools Builds

1) The first thing you need to do is run the autogen.sh script located in the source root. This will run the autotools and generate the build files.

2) Next, switch to your build directory and run configure. You might need to specify the path to a VOL-enabled HDF5 (version 1.13.0 or later) using the --with-hdf5 option. Note that --with-hdf5 needs you to specify the path to the (p)h5cc file (including h5cc).

3) Once configured, you should be able to run make to build the template. Running 'make check' will build the test program and run the test script. The test script and associated program simply make sure that the VOL connector can be registered and loaded dynamically.

### CMake Builds

1) Run ccmake or the CMake GUI and point it at a VOL-enabled HDF5 installation. You may need to switch to see HDF5\_DIR, which you'll need to set to the share/cmake directory of your install. Configure and generate.

2) Build the software using 'make', etc.

3) Run the test program using 'make test', 'ctest .', etc.
