#include "../src/parallax_vol_connector.h"
#include <H5public.h>
#include <hdf5.h>
#include <log.h>
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#define PAR_TEST_FILE_NAME_SINGLE "par_test-single.h5"
#define PAR_TEST_FILE_NAME_MULTI "par_test-multi.h5"
#define PAR_TEST_GROUP_NAME "par_group"
#define PAR_TEST_DATASET_NAME "par_dataset"

#define PAR_TEST_FILE_DIM 32
#define PAR_TEST_MEM_DIM 1
#define PAR_TEST_GAP 1

#define PAR_TEST_DOUBLE_VALUE 34.3

static void parh5_test_partial_updates(void)
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
	hsize_t dims[1] = { PAR_TEST_FILE_DIM };

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

	double mem_buf[PAR_TEST_MEM_DIM] = { 0 };
	for (hsize_t dim_0 = 0; dim_0 < PAR_TEST_MEM_DIM; dim_0++)
		mem_buf[dim_0] = PAR_TEST_DOUBLE_VALUE;

	hsize_t mem_dims[1] = { PAR_TEST_MEM_DIM };
	hsize_t mem_start[1] = { 0 };
	hsize_t mem_count[1] = { PAR_TEST_MEM_DIM };
	hid_t mem_dataspace = H5Screate_simple(sizeof(mem_dims) / sizeof(mem_dims[0]), mem_dims, NULL);
	H5Sselect_hyperslab(mem_dataspace, H5S_SELECT_SET, mem_start, NULL, mem_count, NULL);

	int iterations = PAR_TEST_FILE_DIM / (PAR_TEST_MEM_DIM + PAR_TEST_GAP);

	for (int iter = 0; iter < iterations; iter++) {
		// log_debug("Population setting file start at: %d", iter * (PAR_TEST_MEM_DIM + PAR_TEST_GAP));
		hsize_t file_start[1] = { iter * (PAR_TEST_MEM_DIM + PAR_TEST_GAP) };
		hsize_t file_count[1] = { PAR_TEST_MEM_DIM };
		H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, file_start, NULL, file_count, NULL);
		if (H5Dwrite(dataset_id, H5T_NATIVE_DOUBLE, mem_dataspace, dataspace_id, H5P_DEFAULT, mem_buf) < 0) {
			log_fatal("Failed to write");
			_exit(EXIT_FAILURE);
		}
	}
	//Now verification stage

	double mem_buf_ver[PAR_TEST_MEM_DIM] = { 0 };

	hsize_t mem_dims_ver[1] = { PAR_TEST_MEM_DIM };
	hsize_t mem_start_ver[1] = { 0 };
	hsize_t mem_count_ver[1] = { PAR_TEST_MEM_DIM };
	hid_t mem_dataspace_ver = H5Screate_simple(sizeof(mem_dims_ver) / sizeof(mem_dims_ver[0]), mem_dims_ver, NULL);
	H5Sselect_hyperslab(mem_dataspace_ver, H5S_SELECT_SET, mem_start_ver, NULL, mem_count_ver, NULL);

	hsize_t file_dims_ver[1] = { PAR_TEST_FILE_DIM };
	hsize_t file_start_ver[1] = { 0 };
	hsize_t file_count_ver[1] = { 1 };
	hid_t file_dataspace_ver =
		H5Screate_simple(sizeof(file_dims_ver) / sizeof(file_dims_ver[0]), file_dims_ver, NULL);

	for (int iter = 0; iter < PAR_TEST_FILE_DIM; iter++) {
		file_start_ver[0] = iter;
		H5Sselect_hyperslab(file_dataspace_ver, H5S_SELECT_SET, file_start_ver, NULL, file_count_ver, NULL);
		H5Dread(dataset_id, H5T_NATIVE_DOUBLE, mem_dataspace_ver, file_dataspace_ver, H5P_DEFAULT, mem_buf_ver);
		if (0 == iter % 2) {
			if (mem_buf_ver[0] == PAR_TEST_DOUBLE_VALUE)
				continue;
			log_fatal("TEST failed, at iter %d mem_buf should be %lf instead its value is %lf", iter,
				  PAR_TEST_DOUBLE_VALUE, mem_buf_ver[0]);
			_exit(EXIT_FAILURE);
		}
		if (mem_buf_ver[0] == 0)
			continue;

		log_fatal("TEST failed at iter: %d, mem_buf should be %lf instead its value is %lf", iter, 0.0,
			  mem_buf_ver[0]);
		_exit(EXIT_FAILURE);
	}

	log_info("TEST multi update SUCCESS!");
	// Close the dataset, dataspace, group, and file
	H5Dclose(dataset_id);
	H5Sclose(dataspace_id);
	H5Gclose(group_id);
	H5Fclose(file_id);
}

#include <H5Ipublic.h>
typedef struct H5S_t H5S_t;
herr_t H5S_select_construct_projection(H5S_t *base_space, H5S_t **new_space_ptr, unsigned new_space_rank,
				       hsize_t element_size, ptrdiff_t *buf_adj);
int main(void)
{
	parh5_test_partial_updates();
	return 0;
}
