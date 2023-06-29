#include "../src/parallax_vol_connector.h"
#include "parallax_server.h"
#include "parh5_macros.h"
#include <H5Fpublic.h>
#include <H5Opublic.h>
#include <H5Ppublic.h>
#include <H5public.h>
#include <getopt.h>
#include <hdf5.h>
#include <log.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct ctp_args {
	hid_t loc_id;
};

// static const char *ctp_to_string(hid_t datatype_id)
// {
// 	int class = H5Tget_class(datatype_id);
// 	const char *types[11] = { "H5T_INTEGER",  "H5T_FLOAT",	"H5T_TIME",	"H5T_STRING",
// 				  "H5T_BITFIELD", "H5T_OPAQUE", "H5T_COMPOUND", "H5T_REFERENCE",
// 				  "H5T_ENUM",	  "H5T_VLEN",	"H5T_ARRAY" };
// 	return types[class];
// }

static void ctp_read_dataset(hid_t dataset_id)
{
	hid_t dataspace = H5Dget_space(dataset_id);

	// Get the dimensions of the dataspace in storage
	int ndims = H5Sget_simple_extent_ndims(dataspace);
	hsize_t file_dims[ndims];
	H5Sget_simple_extent_dims(dataspace, file_dims, NULL);
	log_debug("Storate dimensions are: %d", ndims);
	//count the total elements of the dataset
	hsize_t total_elements = 1;
	for (int i = 0; i < ndims; i++)
		total_elements *= file_dims[i];

	// Create a memory dataspace with the tile dimensions
	hsize_t mem_dims[ndims];
	mem_dims[0] = PARH5_TILE_SIZE;
	for (int i = 1; i < ndims; i++)
		mem_dims[i] = 1;
	hid_t memspace = H5Screate_simple(ndims, mem_dims, NULL);

	//Allocate the data buffer
	hid_t datatype_id = H5Dget_type(dataset_id);
	size_t datatype_size = H5Tget_size(datatype_id);
	char *data = calloc(PARH5_TILE_SIZE, datatype_size);
	memset(data, 0xFF, PARH5_TILE_SIZE * datatype_size);

	hsize_t file_offset[PARH5_MAX_DIMENSIONS] = { 0 };
	hsize_t file_count[PARH5_MAX_DIMENSIONS] = { 0 };
	for (int i = 0; i < ndims; i++)
		file_count[i] = 1;

	//Describe the memspace
	hsize_t mem_offset[PARH5_MAX_DIMENSIONS] = { 0 };
	hsize_t mem_count[PARH5_MAX_DIMENSIONS] = { 0 };
	for (int i = 0; i < ndims; i++)
		mem_count[i] = 1;

	hsize_t elements_fetched = 0;
	while (elements_fetched < total_elements) {
		file_count[0] = file_dims[0] - file_offset[0] < PARH5_TILE_SIZE ? file_dims[0] - file_offset[0] :
										  PARH5_TILE_SIZE;

		mem_count[0] = file_count[0];
		if (H5Sselect_hyperslab(memspace, H5S_SELECT_SET, mem_offset, NULL, mem_count, NULL) < 0) {
			log_fatal("Failed to select mem dataspace");
			_exit(EXIT_FAILURE);
		}

		if (H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, file_offset, NULL, file_count, NULL) < 0) {
			log_fatal("Failed to select file dataspace");
			_exit(EXIT_FAILURE);
		}
		// Read the tile into the buffer
		if (H5Dread(dataset_id, ctp_class_to_memtype(H5Tget_class(datatype_id)), memspace, dataspace,
			    H5P_DEFAULT, data) < 0) {
			log_fatal("Read failed");
			_exit(EXIT_FAILURE);
		}

		parh5_server_write_data(dataset_id, ndims, file_offset, file_count, mem_offset, mem_count, data,
					datatype_size * PARH5_TILE_SIZE);

		for (int i = 0; i < ndims; i++) {
			file_offset[i] += file_count[i];
			if (file_offset[i] >= file_dims[i])
				file_offset[i] = 0;
			file_offset[1 + 1]++;
		}
		elements_fetched += file_count[0];
		log_debug("Total elements: %ld Fetched: %ld Wrote: %ld", total_elements, elements_fetched,
			  file_count[0]);
	}

	free(data);
}

static void ctp_print_usage(void)
{
	log_info("Usage: hdf5_parallax_program -s <src_file_name> -d <dst_file_name>");
}

// static int ctp_transfer_attributes(hid_t src_obj_id, hid_t dst_obj_id)
// {
// 	// Get the number of attributes
// 	int num_attrs = H5Aget_num_attrs(src_obj_id);
// 	if (num_attrs < 0) {
// 		fprintf(stderr, "Error getting number of attributes\n");
// 		return 1;
// 	}

// 	// Iterate over each attribute
// 	for (int i = 0; i < num_attrs; i++) {
// 		// Open the source attribute
// 		hid_t src_attr_id = H5Aopen_idx(src_obj_id, (unsigned int)i);
// 		if (src_attr_id < 0) {
// 			fprintf(stderr, "Error opening attribute\n");
// 			return 1;
// 		}

// 		// Get the attribute name
// 		size_t attr_name_len = H5Aget_name(src_attr_id, 0, NULL);
// 		char *attr_name = malloc((attr_name_len + 1) * sizeof(char));
// 		H5Aget_name(src_attr_id, attr_name_len + 1, attr_name);

// 		// Get the attribute datatype
// 		hid_t attr_type_id = H5Aget_type(src_attr_id);
// 		if (attr_type_id < 0) {
// 			fprintf(stderr, "Error getting attribute datatype\n");
// 			free(attr_name);
// 			H5Aclose(src_attr_id);
// 			return 1;
// 		}

// 		// Get the attribute dataspace
// 		hid_t attr_space_id = H5Aget_space(src_attr_id);
// 		if (attr_space_id < 0) {
// 			fprintf(stderr, "Error getting attribute dataspace\n");
// 			free(attr_name);
// 			H5Tclose(attr_type_id);
// 			H5Aclose(src_attr_id);
// 			return 1;
// 		}

// 		// Create the corresponding attribute in the destination object
// 		hid_t dst_attr_id =
// 			H5Acreate(dst_obj_id, attr_name, attr_type_id, attr_space_id, H5P_DEFAULT, H5P_DEFAULT);
// 		if (dst_attr_id < 0) {
// 			fprintf(stderr, "Error creating attribute: %s\n", attr_name);
// 			free(attr_name);
// 			H5Sclose(attr_space_id);
// 			H5Tclose(attr_type_id);
// 			H5Aclose(src_attr_id);
// 			return 1;
// 		}

// 		// Read the attribute data from the source attribute
// 		void *attr_data = malloc(H5Aget_storage_size(src_attr_id));
// 		if (H5Aread(src_attr_id, attr_type_id, attr_data) < 0) {
// 			fprintf(stderr, "Error reading attribute data: %s\n", attr_name);
// 			free(attr_data);
// 			free(attr_name);
// 			H5Aclose(dst_attr_id);
// 			H5Sclose(attr_space_id);
// 			H5Tclose(attr_type_id);
// 			H5Aclose(src_attr_id);
// 			return 1;
// 		}

// 		// Write the attribute data to the destination attribute
// 		if (H5Awrite(dst_attr_id, attr_type_id, attr_data) < 0) {
// 			fprintf(stderr, "Error writing attribute data: %s\n", attr_name);
// 			free(attr_data);
// 			free(attr_name);
// 			H5Aclose(dst_attr_id);
// 			H5Sclose(attr_space_id);
// 			H5Tclose(attr_type_id);
// 			H5Aclose(src_attr_id);
// 			return 1;
// 		}

// 		// Close the attribute identifiers
// 		free(attr_data);
// 		free(attr_name);
// 		H5Aclose(dst_attr_id);
// 		H5Sclose(attr_space_id);
// 		H5Tclose(attr_type_id);
// 		H5Aclose(src_attr_id);
// 	}

// 	return 0;
// }

herr_t ctp_iterate_groups(hid_t loc_id, const char *name, const H5L_info_t *info, void *opdata)
{
	struct ctp_args *parent = opdata;
	if (info->type != H5L_TYPE_HARD) {
		log_fatal("Sorry only hard links supported XXX TODO XXX");
		_exit(EXIT_FAILURE);
	}

	H5O_info_t link_info = { 0 };
	unsigned flags = H5F_OBJ_ALL;
	if (H5Oget_info_by_name(loc_id, name, &link_info, flags, H5P_DEFAULT) < 0) {
		log_fatal("Error retrieving object info");
		_exit(EXIT_FAILURE);
	}

	// Determine the object type
	if (link_info.type == H5O_TYPE_GROUP) {
		hid_t group_id = H5Gopen(loc_id, name, H5P_DEFAULT);
		if (group_id < 0) {
			log_fatal("Error opening group: name: %s", name);
			_exit(EXIT_FAILURE);
		}

		parh5_server_group_create(parent->loc_id, group_id, name);
		log_debug("Send command to create Parallax group: %s parent: %ld", name, parent->loc_id);
		struct ctp_args children = { .loc_id = group_id };
		if (H5Literate(group_id, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, ctp_iterate_groups, &children) < 0) {
			log_fatal("Error iterating over groups of src hdf5 file");
			_exit(EXIT_FAILURE);
		}
		return H5_ITER_CONT;
	} else if (link_info.type == H5O_TYPE_DATASET) {
		log_debug("Link: %s is a dataset", name);
		// Open the dataset
		hid_t dataset_id = H5Dopen(loc_id, name, H5P_DEFAULT);
		if (dataset_id < 0) {
			log_fatal("Error opening dataset");
			_exit(EXIT_FAILURE);
		}

		parh5_server_dataset_create(parent->loc_id, dataset_id, name);
		log_debug("Send command to create dataset: %s", name);

		ctp_read_dataset(dataset_id);
		// Close the dataset, datatype, and dataspace
		H5Dclose(dataset_id);
		return H5_ITER_CONT;
	} else {
		log_fatal("Link: %s is unknown XXX TODO XXX", name);
		_exit(EXIT_FAILURE);
	}

	return H5_ITER_STOP;
}

int main(int argc, char *argv[])
{
	parh5_server_init();
	char *src_file_name = NULL;
	char *dst_file_name = NULL;

	int option;
	while ((option = getopt(argc, argv, "s:d:")) != -1) {
		switch (option) {
		case 's':
			src_file_name = optarg;
			log_debug("Source file name is %s", src_file_name);
			break;
		case 'd':
			dst_file_name = optarg;
			log_debug("Dst file name is %s", dst_file_name);
			break;
		default:
			ctp_print_usage();
			_exit(EXIT_FAILURE);
		}
	}

	if (src_file_name == NULL || dst_file_name == NULL) {
		ctp_print_usage();
		_exit(EXIT_FAILURE);
	}

	// Open the source HDF5 file
	hid_t src_file_id = H5Fopen(src_file_name, H5F_ACC_RDONLY, H5P_DEFAULT);
	if (src_file_id < 0) {
		log_fatal("Error opening native source file: %s", src_file_name);
		_exit(EXIT_FAILURE);
	}
	log_info("Opened native source file: %s", src_file_name);

	struct ctp_args args = { .loc_id = src_file_id };
	parh5_server_file_create(0, src_file_id, dst_file_name);
	log_debug("Send command to create file: %s", dst_file_name);

	//Iterate all groups
	if (H5Literate(src_file_id, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, ctp_iterate_groups, (void *)&args) < 0) {
		log_fatal("Error iterating over groups of src hdf5 file: %s", src_file_name);
		_exit(EXIT_FAILURE);
	}

	// // Transfer datasets and attributes from the source file to the destination file
	// if (ctp_copy_data(src_file_id, dst_file_id) != 0) {
	// 	H5Fclose(dst_file_id);
	// 	H5Fclose(src_file_id);
	// 	log_fatal("Failed to copy src file: %s to dst file: %s", src_file_name, dst_file_name);
	// 	_exit(EXIT_FAILURE);
	// }

	// Close the source file
	H5Fclose(src_file_id);

	log_info("Data and attributes have been successfully transferred from %s to %s using Parallax plugin.",
		 src_file_name, dst_file_name);
	parh5_server_close();

	return EXIT_SUCCESS;
}
