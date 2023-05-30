#include "../src/parallax_vol_connector.h"
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

bool par_test_write_array(char *file_name, char *group_name, char *dataset_name)
{
	double data[PAR_TEST_DIMENSION_1][PAR_TEST_DIMENSION_2][PAR_TEST_DIMENSION_3];

	// Create a new HDF5 file
	hid_t file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

	// Create a group within the file
	hid_t group_id = H5Gcreate2(file_id, group_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

	// Define the dimensions of the dataset
	hsize_t dims[3] = { PAR_TEST_DIMENSION_1, PAR_TEST_DIMENSION_2, PAR_TEST_DIMENSION_3 };

	// Create a dataspace for the dataset
	hid_t dataspace_id = H5Screate_simple(3, dims, NULL);

	// Create the dataset within the group
	hid_t dataset_id = H5Dcreate2(group_id, dataset_name, H5T_NATIVE_DOUBLE, dataspace_id, H5P_DEFAULT, H5P_DEFAULT,
				      H5P_DEFAULT);

	// Initialize the data array with value 1.0
	for (int dim_id_1 = 0; dim_id_1 < PAR_TEST_DIMENSION_1; dim_id_1++) {
		for (int dim_id_2 = 0; dim_id_2 < PAR_TEST_DIMENSION_2; dim_id_2++) {
			for (int dim_id_3 = 0; dim_id_3 < PAR_TEST_DIMENSION_3; dim_id_3++) {
				data[dim_id_1][dim_id_2][dim_id_3] = 1.0;
			}
		}
	}

	// Write the data to the dataset
	H5Dwrite(dataset_id, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);

	// Close the dataset, dataspace, group, and file
	H5Dclose(dataset_id);
	H5Sclose(dataspace_id);
	H5Gclose(group_id);
	H5Fclose(file_id);

	return 0;
}

static bool par_test_read_array(char *file_name, char *group_name, char *dataset_name)
{
	hid_t file_id, group_id, dataset_id, dataspace_id;
	double data[PAR_TEST_DIMENSION_1][PAR_TEST_DIMENSION_2][PAR_TEST_DIMENSION_3] = { 0 };

	// Open the HDF5 file
	file_id = H5Fopen(file_name, H5F_ACC_RDONLY, H5P_DEFAULT);

	// Open the group within the file
	group_id = H5Gopen2(file_id, group_name, H5P_DEFAULT);

	// Open the dataset within the group
	dataset_id = H5Dopen2(group_id, dataset_name, H5P_DEFAULT);

	// Get the dataspace of the dataset
	dataspace_id = H5Dget_space(dataset_id);

	// Read the data from the dataset
	H5Dread(dataset_id, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);

	// Print the values of the array
	for (int dim_id_1 = 0; dim_id_1 < PAR_TEST_DIMENSION_1; dim_id_1++) {
		for (int dim_id_2 = 0; dim_id_2 < PAR_TEST_DIMENSION_2; dim_id_2++) {
			for (int dim_id_3 = 0; dim_id_3 < PAR_TEST_DIMENSION_3; dim_id_3++) {
				if (data[dim_id_1][dim_id_2][dim_id_3] != 1.0) {
					log_fatal("Corrupted array should have been 1.0 instead got %lf",
						  data[dim_id_1][dim_id_2][dim_id_3]);
					_exit(EXIT_FAILURE);
				}
			}
		}
	}

	// Close the dataset, dataspace, group, and file
	H5Dclose(dataset_id);
	H5Sclose(dataspace_id);
	H5Gclose(group_id);
	H5Fclose(file_id);
	return true;
}

int main(void)
{
	/* Register the connector by name */
	hid_t vol_id = { 0 };
	if ((vol_id = H5VLregister_connector_by_name(PARALLAX_VOL_CONNECTOR_NAME, H5P_DEFAULT)) < 0) {
		fprintf(stderr, "Failed to register connector %s", PARALLAX_VOL_CONNECTOR_NAME);
		_exit(EXIT_FAILURE);
	}
	par_test_write_array(PAR_TEST_FILE_NAME, PAR_TEST_GROUP_NAME, PAR_TEST_DATASET_NAME);
	par_test_read_array(PAR_TEST_FILE_NAME, PAR_TEST_GROUP_NAME, PAR_TEST_DATASET_NAME);
	log_info("Test SUCCESS!");
	// Create file
	// log_debug("*****Creating file....*******");
	// hid_t fapl_id = H5Pcreate(H5P_FILE_ACCESS);
	// H5Pset_vol(fapl_id, vol_id, NULL);
	// hid_t file_id = H5Fcreate("my_parallax_file.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id);
	// log_debug("*****Creating file...SUCCESS.*******");

	// Create group
	// log_debug("*****Creating group...*******");
	// hid_t lcpl_id = H5Pcreate(H5P_LINK_CREATE);
	// hid_t group_id = H5Gcreate2(file_id, "/my_parallax_group", lcpl_id, H5P_DEFAULT, H5P_DEFAULT);
	// log_debug("*****Creating group...SUCCESS*******");

	// Create dataset
	// fprintf(stderr, "*****Creating properties...*******\n");
	// fcpl_id = H5Pcreate(H5P_FILE_CREATE);
	// fprintf(stderr, "*****Creating properties...SUCCESS*******\n");

	// fprintf(stderr, "*****Creating link...*******\n");
	// H5Pset_link_creation_order(fcpl_id,
	//                            H5P_CRT_ORDER_TRACKED | H5P_CRT_ORDER_INDEXED);
	// fprintf(stderr, "*****Creating link...SUCCESS*******\n");

	// log_debug("*****Creating dataset...*******");
	// hid_t dset_id = H5Dcreate2(file_id, "/my_group/my_dataset", H5T_NATIVE_INT, group_id, H5P_DEFAULT, H5P_DEFAULT,
	// 			   H5P_DEFAULT);
	// log_debug("*****Creating dataset...SUCCESS*******");

	// Write data to dataset
	// Close resources
	// status = H5Dclose(dset_id);
	// status = H5Gclose(group_id);
	// status = H5Fclose(file_id);
	// // status = H5Pclose(fcpl_id);
	// status = H5Pclose(fapl_id);
	// status = H5Pclose(lcpl_id);
	herr_t status = H5VLunregister_connector(vol_id);

	return 0;
}
