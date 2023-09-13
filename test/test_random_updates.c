#include <hdf5.h>
#include <log.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define HDF5_ROWS 8192
#define HDF5_COLS 8192
#define SUBCOLS 16
#define HDF5_UPDATE_PERCENTAGE 10
#define CHECK_ERROR(X)                       \
	do {                                 \
		if (X < 0) {                 \
			log_fatal("FAILED"); \
			_exit(EXIT_FAILURE); \
		}                            \
	} while (0);

const char *filename = "super_duper_dataset";
const char *dset_name = "my_dataset";

static void hdf5_create_and_write_array(const char *filename)
{
	herr_t status;

	int data[1][SUBCOLS] = { 0 }; // 2D array initialization with zeros

	// Create a new HDF5 file
	hid_t file_id = H5Fcreate(filename, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

	// Create the data space for the dataset
	hsize_t dims[2] = { HDF5_ROWS, HDF5_COLS };
	hsize_t dataspace_id = H5Screate_simple(2, dims, NULL);
	// Create a 2D memory dataspace
	hsize_t mem_dims[2] = { 1, SUBCOLS };
	hid_t memspace_id = H5Screate_simple(2, mem_dims, NULL);

	// Define chunking property
	hid_t cparms = H5Pcreate(H5P_DATASET_CREATE);
	hsize_t chunk_dims[2] = { 1, SUBCOLS }; // Chunk dimensions
	H5Pset_chunk(cparms, 2, chunk_dims);
	hid_t dataset_id =
		H5Dcreate2(file_id, dset_name, H5T_NATIVE_INT, dataspace_id, H5P_DEFAULT, cparms, H5P_DEFAULT);
	CHECK_ERROR(dataset_id)

	for (int row_id = 0; row_id < HDF5_ROWS; row_id++) {
		for (int col_id = 0; col_id < HDF5_COLS; col_id += SUBCOLS) {
			// log_debug("start is at: [%d][%d][%d][%d]", idx_row, idx_column,i,j);
			hsize_t coords[] = { row_id, col_id }; // Starting point of the subarray
			hsize_t count_op[] = { 1, SUBCOLS }; // Size of the subarray
			H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, coords, NULL, count_op, NULL);
			// Write the dataset
			status = H5Dwrite(dataset_id, H5T_NATIVE_INT, memspace_id, dataspace_id, H5P_DEFAULT, data);
			CHECK_ERROR(status)
		}
	}

	// Close resources
	H5Dclose(dataset_id);
	H5Pclose(cparms);
	H5Sclose(dataspace_id);
	H5Fclose(file_id);
}

// Function to randomly update an element
static void hdf5_random_update(const char *filename)
{
	// Seed random number generator
	srand(-1);
	uint64_t num_updates = (HDF5_ROWS * HDF5_COLS * HDF5_UPDATE_PERCENTAGE) / 100;
	// Set the coordinates

	log_info("Updating array total updates are: %lu", num_updates);

	// Open an existing HDF5 file
	hid_t file_id = H5Fopen(filename, H5F_ACC_RDWR, H5P_DEFAULT);
	CHECK_ERROR(file_id)

	// Open an existing dataset in the file
	hid_t dataset_id = H5Dopen2(file_id, dset_name, H5P_DEFAULT);
	CHECK_ERROR(dataset_id)

	// Create the data space for the dataset
	hsize_t dims[2] = { HDF5_ROWS, HDF5_COLS };
	hsize_t dataspace_id = H5Screate_simple(2, dims, NULL);
	// Create a 2D memory dataspace
	hsize_t mem_dims[2] = { 1, 1 };
	hid_t memspace_id = H5Screate_simple(2, mem_dims, NULL);

	for (uint64_t i = 0; i < num_updates; i++) {
		// Randomly choose a row and column
		int random_row = rand() % HDF5_ROWS;
		int random_col = rand() % HDF5_COLS;
		int row_coords[] = { random_row };
		int cols_coords[] = { random_col };
		// Data to write (update)
		int data[1] = { 1983 };
		// log_debug("start is at: [%d][%d][%d][%d]", idx_row, idx_column,i,j);
		hsize_t coords[] = { random_row, random_col }; // Starting point of the subarray
		hsize_t count_op[] = { 1, 1 }; // Size of the subarray

		// Define chunking property
		hid_t cparms = H5Pcreate(H5P_DATASET_CREATE);
		hsize_t chunk_dims[2] = { 1, SUBCOLS }; // Chunk dimensions
		H5Pset_chunk(cparms, 2, chunk_dims);
		H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, coords, NULL, count_op, NULL);
		// Write the dataset
		hid_t status = H5Dwrite(dataset_id, H5T_NATIVE_INT, memspace_id, dataspace_id, H5P_DEFAULT, data);
		CHECK_ERROR(status)
	}
	// Close the dataset and file
	H5Dclose(dataset_id);
	H5Fclose(file_id);
}

int main(int argc, char **argv)
{
	char *operation = NULL;

	// Parse command-line arguments
	for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-w") == 0 && i + 1 < argc) {
			operation = argv[i + 1];
			i++; // skip the next argument since we've just processed it
		}
	}

	if (!operation) {
		log_fatal("Missing mandatory -w parameter");
		_exit(EXIT_FAILURE);
	}

	if (strcmp(operation, "write") != 0 && strcmp(operation, "update") != 0) {
		log_fatal("Invalid value for -w. Expected 'write' or 'update'.");
		_exit(EXIT_FAILURE);
	}

	// Now, based on the value of operation, perform your tasks
	if (strcmp(operation, "write") == 0) {
		log_info("Executing write workload");
		hdf5_create_and_write_array(filename);
		return 1;
	}
	log_info("Executing update workload");
	hdf5_random_update(filename);

	return 1;
}
