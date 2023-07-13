#include "../src/parallax_vol_connector.h"
#include "H5Ppublic.h"
#include <H5public.h>
#include <hdf5.h>
#include <log.h>
#include <stdlib.h>
#include <unistd.h>
#define PAR_TEST_FILE_NAME "par_test.h5"
#define PAR_TEST_GROUP_NAME "par_group"
#define PAR_TEST_DATASET_NAME "par_dataset"
#define PAR_TEST_DIMENSION_1 100
#define PAR_TEST_DIMENSION_2 50
#define PAR_TEST_DIMENSION_3 20
#define PAR_TEST_DIMENSION_4 20
#define PAR_TEST_DOUBLE_VALUE 0.0

int main(void)
{
	// Create a new HDF5 file
	hid_t fapl_id = H5Pcreate(H5P_FILE_ACCESS);
	log_debug("Creating file: %s.....", PAR_TEST_FILE_NAME);
	hid_t file_id = H5Fcreate(PAR_TEST_FILE_NAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id);
	if (file_id <= 0) {
		log_fatal("File creation failed");
		_exit(EXIT_FAILURE);
	}
	log_debug("Created file: %s SUCCESS", PAR_TEST_FILE_NAME);

	// Create a group within the file
	log_debug("Creating group: %s.....", PAR_TEST_GROUP_NAME);
	hid_t group_id = H5Gcreate(file_id, PAR_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	if (group_id <= 0) {
		log_fatal("Group creation failed");
		_exit(EXIT_FAILURE);
	}

	log_debug("Created group: %s SUCCESSFULLY", PAR_TEST_GROUP_NAME);

	// Define the dimensions of the dataset
	hsize_t dims[4] = { PAR_TEST_DIMENSION_1, PAR_TEST_DIMENSION_2, PAR_TEST_DIMENSION_3, PAR_TEST_DIMENSION_4 };

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
	float mem_buf_a[PAR_TEST_DIMENSION_1][PAR_TEST_DIMENSION_3] = { 0 };
	for (hsize_t row_id = 0; row_id < PAR_TEST_DIMENSION_1; row_id++) {
		for (hsize_t col_id = 0; col_id < PAR_TEST_DIMENSION_3; col_id++) {
			mem_buf_a[row_id][col_id] = PAR_TEST_DOUBLE_VALUE;
		}
	}

	hsize_t mem_dims_a[2] = { PAR_TEST_DIMENSION_1, PAR_TEST_DIMENSION_3 };
	hsize_t mem_start_a[2] = { 0, 0 };
	hsize_t mem_count_a[2] = { PAR_TEST_DIMENSION_1, PAR_TEST_DIMENSION_3 };
	hid_t mem_dataspace_a = H5Screate_simple(sizeof(mem_dims_a) / sizeof(mem_dims_a[0]), mem_dims_a, NULL);
	H5Sselect_hyperslab(mem_dataspace_a, H5S_SELECT_SET, mem_start_a, NULL, mem_count_a, NULL);

	hsize_t file_start_a[4] = { 0, 0, 0, 0 };
	hsize_t file_count_a[4] = { PAR_TEST_DIMENSION_1, 1, PAR_TEST_DIMENSION_3, 1 };
	H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, file_start_a, NULL, file_count_a, NULL);
	log_debug("Writing 1st and 3rd dimensions of dataset...");
	H5Dwrite(dataset_id, H5T_NATIVE_FLOAT, mem_dataspace_a, dataspace_id, H5P_DEFAULT, mem_buf_a);
	log_debug("Wrote 1st and 3rd dimensions of dataset SUCCESSFULLY");

	//----> Write 2nd and 3rd dimensions from memory to storage
	float mem_buf_b[PAR_TEST_DIMENSION_2][PAR_TEST_DIMENSION_4] = { 0 };
	for (hsize_t row_id = 0; row_id < PAR_TEST_DIMENSION_2; row_id++) {
		for (hsize_t col_id = 0; col_id < PAR_TEST_DIMENSION_4; col_id++) {
			mem_buf_b[row_id][col_id] = PAR_TEST_DOUBLE_VALUE;
		}
	}

	hsize_t mem_dims_b[2] = { PAR_TEST_DIMENSION_1, PAR_TEST_DIMENSION_3 };
	hsize_t mem_start_b[2] = { 0, 0 };
	hsize_t mem_count_b[2] = { PAR_TEST_DIMENSION_2, PAR_TEST_DIMENSION_4 };
	hid_t mem_dataspace_b = H5Screate_simple(sizeof(mem_dims_b) / sizeof(mem_dims_b[0]), mem_dims_b, NULL);
	H5Sselect_hyperslab(mem_dataspace_b, H5S_SELECT_SET, mem_start_b, NULL, mem_count_b, NULL);

	hsize_t file_start_b[4] = { 0, 0, 0, 0 };
	hsize_t file_count_b[4] = { 1, PAR_TEST_DIMENSION_2, 1, PAR_TEST_DIMENSION_4 };
	H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, file_start_b, NULL, file_count_b, NULL);
	// Write the data to the dataset
	log_debug("Writing 1st and 3rd dimensions of dataset...");
	H5Dwrite(dataset_id, H5T_NATIVE_FLOAT, mem_dataspace_b, dataspace_id, H5P_DEFAULT, mem_buf_b);
	log_debug("Wrote 2nd and 4th dimensions SUCCESSFULLY");

	//Now verification stage

	float mem_buf_c[PAR_TEST_DIMENSION_1][PAR_TEST_DIMENSION_2][PAR_TEST_DIMENSION_3][PAR_TEST_DIMENSION_4] = { 0 };

	hsize_t mem_dims_c[4] = { PAR_TEST_DIMENSION_1, PAR_TEST_DIMENSION_2, PAR_TEST_DIMENSION_3,
				  PAR_TEST_DIMENSION_4 };
	hsize_t mem_start_c[4] = { 0, 0, 0, 0 };
	hsize_t mem_count_c[4] = { PAR_TEST_DIMENSION_1, PAR_TEST_DIMENSION_2, PAR_TEST_DIMENSION_3,
				   PAR_TEST_DIMENSION_4 };
	hid_t mem_dataspace_c = H5Screate_simple(sizeof(mem_dims_c) / sizeof(mem_dims_c[0]), mem_dims_c, NULL);
	H5Sselect_hyperslab(mem_dataspace_b, H5S_SELECT_SET, mem_start_c, NULL, mem_count_c, NULL);

	hsize_t file_start_c[4] = { 0, 0, 0, 0 };
	hsize_t file_count_c[4] = { PAR_TEST_DIMENSION_1, PAR_TEST_DIMENSION_2, PAR_TEST_DIMENSION_3,
				    PAR_TEST_DIMENSION_4 };
	H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, file_start_c, NULL, file_count_c, NULL);

	log_debug("Reading and verifying the whole 4 dimensional array...");
	H5Dread(dataset_id, H5T_NATIVE_FLOAT, mem_dataspace_c, dataspace_id, H5P_DEFAULT, mem_buf_c);

	for (hsize_t d1 = 0; d1 < PAR_TEST_DIMENSION_1; d1++) {
		for (hsize_t d2 = 0; d2 < PAR_TEST_DIMENSION_2; d2++) {
			for (hsize_t d3 = 0; d3 < PAR_TEST_DIMENSION_3; d3++) {
				for (hsize_t d4 = 0; d4 < PAR_TEST_DIMENSION_4; d4++) {
					if (mem_buf_c[d1][d2][d3][d4] == PAR_TEST_DOUBLE_VALUE)
						continue;
					log_fatal(
						"Corrupted array element a[%ld][%ld][%ld][%ld] = %lf whereas it should have been %lf",
						d1, d2, d3, d4, mem_buf_c[d1][d2][d3][d4], PAR_TEST_DOUBLE_VALUE);
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

	return 0;
}
