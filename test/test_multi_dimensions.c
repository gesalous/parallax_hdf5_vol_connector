#include "../src/parallax_vol_connector.h"
#include "H5Ppublic.h"
#include <H5public.h>
#include <hdf5.h>
#include <log.h>
#include <stdlib.h>
#include <unistd.h>
#define PAR_TEST_FILE_NAME_SINGLE "par_test-single.h5"
#define PAR_TEST_FILE_NAME_MULTI "par_test-multi.h5"
#define PAR_TEST_GROUP_NAME "par_group"
#define PAR_TEST_DATASET_NAME "par_dataset"

#define PAR_TEST_DIM_0 2
#define PAR_TEST_DIM_1 2
#define PAR_TEST_DIM_2 2
#define PAR_TEST_DIM_3 126

#define PAR_TEST_DOUBLE_VALUE 34.3

static void parh5_test_single_dims(void)
{
	// Create a new HDF5 file
	hid_t fapl_id = H5Pcreate(H5P_FILE_ACCESS);
	log_debug("Creating file: %s.....", PAR_TEST_FILE_NAME_SINGLE);
	hid_t file_id = H5Fcreate(PAR_TEST_FILE_NAME_SINGLE, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id);
	if (file_id <= 0) {
		log_fatal("File creation failed");
		_exit(EXIT_FAILURE);
	}
	log_debug("Created file: %s SUCCESS", PAR_TEST_FILE_NAME_SINGLE);

	// Create a group within the file
	log_debug("Creating group: %s.....", PAR_TEST_GROUP_NAME);
	hid_t group_id = H5Gcreate(file_id, PAR_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	if (group_id <= 0) {
		log_fatal("Group creation failed");
		_exit(EXIT_FAILURE);
	}

	log_debug("Created group: %s SUCCESSFULLY", PAR_TEST_GROUP_NAME);

	// Define the dimensions of the dataset
	hsize_t dims[4] = { PAR_TEST_DIM_0, PAR_TEST_DIM_1, PAR_TEST_DIM_2, PAR_TEST_DIM_3 };

	// Create a dataspace for the dataset
	hid_t dataspace_id = H5Screate_simple(sizeof(dims) / sizeof(hsize_t), dims, NULL);
	if (dataspace_id < 0) {
		log_fatal("Failed to create dataspace");
		_exit(EXIT_FAILURE);
	}

	log_debug("Creating dataset: %s.....", PAR_TEST_DATASET_NAME);
	// Create the dataset within the group
	hid_t dataset_id = H5Dcreate2(group_id, PAR_TEST_DATASET_NAME, H5T_NATIVE_DOUBLE, dataspace_id, H5P_DEFAULT,
				      H5P_DEFAULT, H5P_DEFAULT);
	log_debug("Created dataset: %s SUCCESSFULLY", PAR_TEST_DATASET_NAME);
	double value = PAR_TEST_DOUBLE_VALUE;

	double mem_buf[PAR_TEST_DIM_3][PAR_TEST_DIM_2][PAR_TEST_DIM_1][PAR_TEST_DIM_0] = { 0 };
	for (hsize_t dim_3 = 0; dim_3 < PAR_TEST_DIM_3; dim_3++) {
		for (hsize_t dim_2 = 0; dim_2 < PAR_TEST_DIM_2; dim_2++) {
			for (hsize_t dim_1 = 0; dim_1 < PAR_TEST_DIM_1; dim_1++) {
				for (hsize_t dim_0 = 0; dim_0 < PAR_TEST_DIM_0; dim_0++) {
					mem_buf[dim_3][dim_2][dim_1][dim_0] = PAR_TEST_DOUBLE_VALUE;
				}
			}
		}
	}

	hsize_t mem_dims[4] = { PAR_TEST_DIM_0, PAR_TEST_DIM_1, PAR_TEST_DIM_2, PAR_TEST_DIM_3 };
	hsize_t mem_start[4] = { 0, 0, 0, 0 };
	hsize_t mem_count[4] = { PAR_TEST_DIM_0, PAR_TEST_DIM_1, PAR_TEST_DIM_2, PAR_TEST_DIM_3 };
	hid_t mem_dataspace = H5Screate_simple(sizeof(mem_dims) / sizeof(mem_dims[0]), mem_dims, NULL);
	H5Sselect_hyperslab(mem_dataspace, H5S_SELECT_SET, mem_start, NULL, mem_count, NULL);

	hsize_t file_start[4] = { 0, 0, 0, 0 };
	hsize_t file_count[4] = { PAR_TEST_DIM_0, PAR_TEST_DIM_1, PAR_TEST_DIM_2, PAR_TEST_DIM_3 };
	H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, file_start, NULL, file_count, NULL);
	log_debug("Writing 1st, 2nd, 3rd, and 4th (ALL) dimensions of dataset...");
	H5Dwrite(dataset_id, H5T_NATIVE_DOUBLE, mem_dataspace, dataspace_id, H5P_DEFAULT, mem_buf);
	log_debug("Wrote 1st, 2nd, 3rd, and 4th (ALL) dimensions of dataset...SUCCESSFULLY");

	//Now verification stage

	log_debug("Reading and verifying the whole 4 dimensional array...");
	H5Dread(dataset_id, H5T_NATIVE_DOUBLE, mem_dataspace, dataspace_id, H5P_DEFAULT, mem_buf);

	for (hsize_t dim_3 = 0; dim_3 < PAR_TEST_DIM_3; dim_3++) {
		for (hsize_t dim_2 = 0; dim_2 < PAR_TEST_DIM_2; dim_2++) {
			for (hsize_t dim_1 = 0; dim_1 < PAR_TEST_DIM_1; dim_1++) {
				for (hsize_t dim_0 = 0; dim_0 < PAR_TEST_DIM_0; dim_0++) {
					if (mem_buf[dim_3][dim_2][dim_1][dim_0] == PAR_TEST_DOUBLE_VALUE)
						continue;
					log_fatal(
						"Corrupted array element a[%ld][%ld][%ld][%ld] = %lf whereas it should have been %lf",
						dim_3, dim_2, dim_1, dim_0, mem_buf[dim_3][dim_2][dim_1][dim_0],
						PAR_TEST_DOUBLE_VALUE);
					_exit(EXIT_FAILURE);
				}
			}
		}
	}
	log_info("TEST single dimension writes/reads success!");
	// Close the dataset, dataspace, group, and file
	H5Dclose(dataset_id);
	H5Sclose(dataspace_id);
	H5Gclose(group_id);
	H5Fclose(file_id);
}

static void parh5_test_multi_dims(void)
{
	// Create a new HDF5 file
	hid_t fapl_id = H5Pcreate(H5P_FILE_ACCESS);
	log_debug("Creating file: %s.....", PAR_TEST_FILE_NAME_MULTI);
	hid_t file_id = H5Fcreate(PAR_TEST_FILE_NAME_MULTI, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id);
	if (file_id <= 0) {
		log_fatal("File creation failed");
		_exit(EXIT_FAILURE);
	}
	log_debug("Created file: %s SUCCESS", PAR_TEST_FILE_NAME_MULTI);

	// Create a group within the file
	log_debug("Creating group: %s.....", PAR_TEST_GROUP_NAME);
	hid_t group_id = H5Gcreate(file_id, PAR_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	if (group_id <= 0) {
		log_fatal("Group creation failed");
		_exit(EXIT_FAILURE);
	}

	log_debug("Created group: %s SUCCESSFULLY", PAR_TEST_GROUP_NAME);

	// Define the dimensions of the dataset
	hsize_t dims[4] = { PAR_TEST_DIM_0, PAR_TEST_DIM_1, PAR_TEST_DIM_2, PAR_TEST_DIM_3 };

	// Create a dataspace for the dataset
	hid_t dataspace_id = H5Screate_simple(sizeof(dims) / sizeof(hsize_t), dims, NULL);
	if (dataspace_id < 0) {
		log_fatal("Failed to create dataspace");
		_exit(EXIT_FAILURE);
	}

	log_debug("Creating dataset: %s.....", PAR_TEST_DATASET_NAME);
	// Create the dataset within the group
	hid_t dataset_id = H5Dcreate2(group_id, PAR_TEST_DATASET_NAME, H5T_NATIVE_DOUBLE, dataspace_id, H5P_DEFAULT,
				      H5P_DEFAULT, H5P_DEFAULT);
	log_debug("Created dataset: %s SUCCESSFULLY", PAR_TEST_DATASET_NAME);
	double value = PAR_TEST_DOUBLE_VALUE;

	//---> Write 1st and 3rd dimensions from memory to storage
	double mem_buf_a[PAR_TEST_DIM_1][PAR_TEST_DIM_0] = { 0 };
	for (hsize_t dim_1 = 0; dim_1 < PAR_TEST_DIM_1; dim_1++) {
		for (hsize_t dim_0 = 0; dim_0 < PAR_TEST_DIM_0; dim_0++) {
			mem_buf_a[dim_1][dim_0] = PAR_TEST_DOUBLE_VALUE;
		}
	}

	hsize_t mem_dims_a[2] = { PAR_TEST_DIM_0, PAR_TEST_DIM_1 };
	hsize_t mem_start_a[2] = { 0, 0 };
	hsize_t mem_count_a[2] = { PAR_TEST_DIM_0, PAR_TEST_DIM_1 };
	hid_t mem_dataspace_a = H5Screate_simple(sizeof(mem_dims_a) / sizeof(mem_dims_a[0]), mem_dims_a, NULL);
	H5Sselect_hyperslab(mem_dataspace_a, H5S_SELECT_SET, mem_start_a, NULL, mem_count_a, NULL);

	for (int dim_3 = 0; dim_3 < PAR_TEST_DIM_3; dim_3++) {
		for (int dim_2 = 0; dim_2 < PAR_TEST_DIM_2; dim_2++) {
			hsize_t file_start_a[4] = { 0, 0, dim_2, dim_3 };
			hsize_t file_count_a[4] = { PAR_TEST_DIM_0, PAR_TEST_DIM_1, 1, 1 };
			H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, file_start_a, NULL, file_count_a, NULL);
			H5Dwrite(dataset_id, H5T_NATIVE_DOUBLE, mem_dataspace_a, dataspace_id, H5P_DEFAULT, mem_buf_a);
		}
	}
	//Now verification stage

	double mem_buf_c[PAR_TEST_DIM_3][PAR_TEST_DIM_2][PAR_TEST_DIM_1][PAR_TEST_DIM_0] = { 0 };

	hsize_t mem_dims_c[4] = { PAR_TEST_DIM_0, PAR_TEST_DIM_1, PAR_TEST_DIM_2, PAR_TEST_DIM_3 };
	hsize_t mem_start_c[4] = { 0, 0, 0, 0 };
	hsize_t mem_count_c[4] = { PAR_TEST_DIM_0, PAR_TEST_DIM_1, PAR_TEST_DIM_2, PAR_TEST_DIM_3 };
	hid_t mem_dataspace_c = H5Screate_simple(sizeof(mem_dims_c) / sizeof(mem_dims_c[0]), mem_dims_c, NULL);
	H5Sselect_hyperslab(mem_dataspace_c, H5S_SELECT_SET, mem_start_c, NULL, mem_count_c, NULL);

	hsize_t file_start_c[4] = { 0, 0, 0, 0 };
	hsize_t file_count_c[4] = { PAR_TEST_DIM_0, PAR_TEST_DIM_1, PAR_TEST_DIM_2, PAR_TEST_DIM_3 };
	H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, file_start_c, NULL, file_count_c, NULL);

	log_debug("Reading and verifying the whole 4 dimensional array...");
	H5Dread(dataset_id, H5T_NATIVE_DOUBLE, mem_dataspace_c, dataspace_id, H5P_DEFAULT, mem_buf_c);

	for (hsize_t dim_3 = 0; dim_3 < PAR_TEST_DIM_3; dim_3++) {
		for (hsize_t dim_2 = 0; dim_2 < PAR_TEST_DIM_2; dim_2++) {
			for (hsize_t dim_1 = 0; dim_1 < PAR_TEST_DIM_1; dim_1++) {
				for (hsize_t dim_0 = 0; dim_0 < PAR_TEST_DIM_0; dim_0++) {
					if (mem_buf_c[dim_3][dim_2][dim_1][dim_0] == PAR_TEST_DOUBLE_VALUE)
						continue;
					log_fatal(
						"Corrupted array element a[%ld][%ld][%ld][%ld] = %lf whereas it should have been %lf",
						dim_3, dim_2, dim_1, dim_0, mem_buf_c[dim_3][dim_2][dim_1][dim_0],
						PAR_TEST_DOUBLE_VALUE);
					_exit(EXIT_FAILURE);
				}
			}
		}
	}
	log_info("TEST multidimensional writes/reads success!");
	// Close the dataset, dataspace, group, and file
	H5Dclose(dataset_id);
	H5Sclose(dataspace_id);
	H5Gclose(group_id);
	H5Fclose(file_id);
}

int main(void)
{
	log_info("Testing writing an array with all of its dimensions");
	parh5_test_single_dims();
	log_info("Testing writing an array with a subset of its dimensions");
	parh5_test_multi_dims();
	return 0;
}
