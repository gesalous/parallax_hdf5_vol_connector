#include "../src/parallax_vol_connector.h"
#include "H5Ipublic.h"
#include "hdf5.h"
#include <H5Gpublic.h>
#include <log.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define PAR_TEST_FILENAME "scientific_data.h5"
#define PAR_TEST_DIM1 1 //20
#define PAR_TEST_DIM2 1 //100

#define PAR_TEST_CHECK(X, Y)                                \
	if (group_id < 0) {                                 \
		log_fatal("Failed to create group: %s", Y); \
		_exit(EXIT_FAILURE);                        \
	}

int main(void)
{
	/* Register the connector by name */
	// hid_t vol_id = { 0 };
	// if ((vol_id = H5VLregister_connector_by_name(PARALLAX_VOL_CONNECTOR_NAME, H5P_DEFAULT)) < 0) {
	// 	log_fatal("Failed to register connector %s", PARALLAX_VOL_CONNECTOR_NAME);
	// 	_exit(EXIT_FAILURE);
	// }
	hid_t fapl_id = H5Pcreate(H5P_FILE_ACCESS);
	// H5Pset_vol(fapl_id, vol_id, NULL);
	// Create HDF5 file
	hid_t file_id = H5Fcreate(PAR_TEST_FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id);
	if (file_id < 0) {
		log_debug("Failed to create HDF5 file");
		_exit(EXIT_FAILURE);
	}

	// Create day groups
	const char *days[] = { "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday" };
	for (uint32_t i = 0; i < sizeof(days) / sizeof(char *); i++) {
		// Create day group
		hid_t group_id = H5Gcreate2(file_id, days[i], H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
		PAR_TEST_CHECK(group_id, days[i])

		// Create subgroups within day group
		const char *subgroups[] = { "Morning", "Noon", "Afternoon", "Evening" };
		for (uint32_t j = 0; j < sizeof(subgroups) / sizeof(char *); j++) {
			hid_t subgroup_id = H5Gcreate2(group_id, subgroups[j], H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
			PAR_TEST_CHECK(subgroup_id, subgroups[j])
			H5Gclose(subgroup_id);
		}

		H5Gclose(group_id);
	}

	// Create dataset in the Evening subgroup of Sunday
	hid_t group_id = H5Gopen2(file_id, "Sunday/Evening", H5P_DEFAULT);
	PAR_TEST_CHECK(group_id, "Sunday/Evening")

	// Create dataspace for the dataset
	hsize_t dims[2] = { PAR_TEST_DIM1, PAR_TEST_DIM2 };
	hid_t dataspace_id = H5Screate_simple(2, dims, NULL);

	// Create the dataset
	hid_t dataset_id = H5Dcreate2(group_id, "measurements", H5T_NATIVE_FLOAT, dataspace_id, H5P_DEFAULT,
				      H5P_DEFAULT, H5P_DEFAULT);
	PAR_TEST_CHECK(dataset_id, "measurements")
	size_t nelems = dims[0] * dims[1];
	float data[PAR_TEST_DIM1][PAR_TEST_DIM2];
	for (int i = 0; i < PAR_TEST_DIM1; i++)
		for (int j = 0; j < PAR_TEST_DIM2; j++)
			data[i][j] = 2.0f;

	// Write data to the dataset
	H5Dwrite(dataset_id, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);

	// Close dataset, dataspace, group, and file
	// H5Sclose(dataspace_id);
	// H5Dclose(dataset_id);
	// H5Gclose(group_id);
	// H5Fclose(file_id);

	log_debug("Metadata write test passed");

	// Open the HDF5 file
	// file_id = H5Fopen(PAR_TEST_FILENAME, H5F_ACC_RDONLY, fapl_id);
	PAR_TEST_CHECK(file_id, PAR_TEST_FILENAME);

	// Open the "measurements" dataset
	dataset_id = H5Dopen2(file_id, "/Sunday/Evening/measurements", H5P_DEFAULT);
	PAR_TEST_CHECK(dataset_id, "Sunday/Evening/measurements")
	// Get the dataspace of the dataset
	dataspace_id = H5Dget_space(dataset_id);

	// Read the dataset into the 'data' array
	herr_t status = H5Dread(dataset_id, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
	PAR_TEST_CHECK(status, "Failed to read dataset: /Sunday/Evening/measurements");

	// Verify the values in the 'data' array
	for (int i = 0; i < dims[0]; i++) {
		for (int j = 0; j < dims[1]; j++) {
			if (data[i][j] == 2.0f)
				continue;
			log_fatal("Verification failed: data[%d][%d] is not equal to 2.0 instead it is %f\n", i, j,
				  data[i][j]);
			_exit(EXIT_FAILURE);
		}
	}

	log_info("Verification successful: All array values are 1.0\n");
	hid_t r_fid = H5Iget_file_id(dataset_id);
	if (r_fid < 0) {
		log_fatal("Failed to fetch file id in which dataset id is");
		_exit(EXIT_FAILURE);
	}
	// Create the attribute "class"
	hid_t space_id = H5Screate(H5S_SCALAR);
	hid_t type_id = H5Tcopy(H5T_C_S1);
	H5Tset_size(type_id, strlen("kalimera aspronisi"));
	hid_t attr_id = H5Acreate2(dataset_id, "class", type_id, space_id, H5P_DEFAULT, H5P_DEFAULT);

	// Write the attribute value
	const char *attr_value = "kalimera aspronisi";
	status = H5Awrite(attr_id, type_id, attr_value);

	log_info("Finally fetching all objects...");

	ssize_t num_objs = H5Fget_obj_count(file_id, H5F_OBJ_GROUP | H5F_OBJ_DATASET);
	uint32_t dataset_size = H5Sget_simple_extent_npoints(dataspace_id);
	log_info("Dataset size = %u", dataset_size);
	hsize_t object_num = 0;
	if (H5Gget_num_objs(group_id, &object_num) < 0) {
		log_fatal("Failed to get num objs for group");
		_exit(EXIT_FAILURE);
	}
	log_info("---------->NUM OBJS for group are: %ld", object_num);

	// Allocate memory for the object IDs
	hid_t *obj_ids = calloc(num_objs, sizeof(hid_t));
	// Get the object IDs of the groups and datasets
	num_objs = H5Fget_obj_ids(file_id, H5F_OBJ_GROUP | H5F_OBJ_DATASET, num_objs, obj_ids);
	if (num_objs < 0) {
		log_fatal("Failed to get object IDs");
		_exit(EXIT_FAILURE);
	}

	// Iterate over the object IDs and print their names
	for (ssize_t i = 0; i < num_objs; i++) {
		char obj_name[256];
		H5Iget_name(obj_ids[i], obj_name, sizeof(obj_name));
		log_debug("--->Object: %s\n", obj_name);
		// H5Idec_ref(obj_ids[i]);
	}

	// Close the dataset, dataspace, and file
	H5Sclose(dataspace_id);
	H5Dclose(dataset_id);
	H5Fclose(file_id);

	return EXIT_SUCCESS;
}
