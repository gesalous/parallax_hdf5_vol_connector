#include "parallax_vol_dataset.h"
#include "H5Spublic.h"
#include "H5Tpublic.h"
#include "djb2.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_group.h"
#include <assert.h>
#include <log.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct parh5D_dataset {
	parh5_object_e obj_type;
	parh5G_group_t group; /*Where does this dataset belongs*/
	char *name;
	uint64_t dataset_uuid;
};

static uint64_t parh5D_create_uuid(parh5D_dataset_t dataset, const char *name)
{
	const char *group_name = parh5G_get_group_name(dataset->group);
#define PARH5D_UUID_BUFFER_SIZE 128
	uint32_t needed_space = strlen(group_name) + strlen(name) + 1UL;
	dataset->name = calloc(1UL, needed_space);
	memcpy(dataset->name, group_name, strlen(group_name));
	memcpy(&dataset->name[strlen(group_name)], ":", 1UL);
	memcpy(&dataset->name[strlen(group_name) + 1UL], name, strlen(name));
	uint64_t uuid_hash = djb2_hash((unsigned char *)dataset->name, strlen(dataset->name));
	log_debug("uuid = %s hash is %lu", dataset->name, uuid_hash);
	return uuid_hash;
}

void *parh5D_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id,
		    hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)loc_params;
	(void)name;
	(void)lcpl_id;
	(void)type_id;
	(void)space_id;
	(void)dcpl_id;
	(void)dapl_id;
	(void)dxpl_id;
	(void)req;
	parh5_object_e *obj_type = (parh5_object_e *)obj;

	if (PAR_H5_GROUP != *obj_type) {
		log_fatal("Dataset can only be associated with a group object");
		_exit(EXIT_FAILURE);
	}
	parh5G_group_t group = obj;
	const char *error = NULL;
	if (!parh5G_add_dataset(group, name, &error)) {
		log_warn("Cannot add dataset reason: %s", error);
		return NULL;
	}

	parh5D_dataset_t dataset = calloc(1UL, sizeof(struct parh5D_dataset));
	dataset->obj_type = PAR_H5_DATASET;
	dataset->group = group;

	dataset->dataset_uuid = parh5D_create_uuid(dataset, name);
	return dataset;
}

void *parh5D_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id,
		  void **req)
{
	(void)obj;
	(void)loc_params;
	(void)name;
	(void)dapl_id;
	(void)dxpl_id;
	(void)req;
	log_fatal("Dataset: Sorry unimplemented function XXX TODO XXX");
	_exit(EXIT_FAILURE);
	return NULL;
}

herr_t parh5D_read(size_t count, void *dset[], hid_t mem_type_id[], hid_t mem_space_id[], hid_t file_space_id[],
		   hid_t dxpl_id, void *buf[], void **req)
{
	(void)count;
	(void)dset;
	(void)mem_type_id;
	(void)mem_space_id;
	(void)file_space_id;
	(void)dxpl_id;
	(void)buf;
	(void)req;
	log_fatal("Dataset: Sorry unimplemented function XXX TODO XXX");
	_exit(EXIT_FAILURE);
	return 1;
}

static void parh5D_print_datatypes(size_t count, hid_t mem_type_id[])
{
	for (size_t i = 0; i < count; i++) {
		/* Print memory datatype */
		H5Tget_class(mem_type_id[i]);
		switch (H5Tget_class(mem_type_id[i])) {
		case H5T_INTEGER:
			log_debug("Memory Datatype[%zu]: INTEGER", i);
			break;
		case H5T_FLOAT:
			log_debug("Memory Datatype[%zu]: FLOAT", i);
			break;
		case H5T_STRING:
			log_debug("Memory Datatype[%zu]: STRING", i);
			break;
		case H5T_COMPOUND:
			log_debug("Memory Datatype[%zu]: COMPOUND", i);
			break;
		// Add more cases for other datatype classes if needed
		default:
			log_debug("Memory Datatype[%zu]: Unknown", i);
			_exit(EXIT_FAILURE);
		}
	}
}

static void parh5D_print_value(const void *data, int idx, H5T_class_t data_type)
{
	switch (data_type) {
	case H5T_INTEGER: {
		int *data_t = (int *)data;
		printf("[%d] = %d, ", idx, data_t[idx]);
	} break;
	case H5T_FLOAT: {
		float *data_t = (float *)data;
		printf("[%d] = %f, ", idx, data_t[idx]);
	} break;
	case H5T_STRING: {
		char **data_t = (char **)data;
		printf("[%d] = %s, ", idx, data_t[idx]);
	} break;
	default:
		log_fatal("Memory Datatype[%d]: Unknown Brr", data_type);
		assert(0);
		_exit(EXIT_FAILURE);
	}
}

static void parh5D_print_data(const void *data, hsize_t start, hsize_t block, hsize_t stride, hsize_t nelems,
			      H5T_class_t data_type)
{
	log_debug("Start %zu block %zu stride %zu nelems %zu", start, block, stride, nelems);
	hsize_t array_id = start;
	while (array_id < start + nelems) {
		// Process a block of items
		for (hsize_t elems_cnt = 0; elems_cnt < block && array_id < start + nelems; elems_cnt++, array_id++) {
			parh5D_print_value(data, array_id, data_type);
		}

		// Skip the specified number of elements
		array_id += stride - 1;
	}
	printf("\n");
}

static void parh5D_print_memspace_info(hid_t mem_space_id, hid_t mem_type_id, const void *buf)
{
#define PARH5D_MAX_DIMS 5
	int ndimensions = H5Sget_simple_extent_ndims(mem_space_id);
	if (ndimensions > PARH5D_MAX_DIMS) {
		log_fatal("Sorry cannot print more than %d DIMS", PARH5D_MAX_DIMS);
		_exit(EXIT_FAILURE);
	}
	hsize_t start[PARH5D_MAX_DIMS] = { 0 };
	hsize_t block[PARH5D_MAX_DIMS] = { 0 };
	hsize_t stride[PARH5D_MAX_DIMS] = { 0 };
	hsize_t count[PARH5D_MAX_DIMS] = { 0 };

	H5Sget_regular_hyperslab(mem_space_id, start, stride, count, block);

	/* Print memory dataspace start index, block, and stride */
	for (int dimension_id = 0; dimension_id < ndimensions; dimension_id++) {
		log_debug("Info for memoryspace dimension %d", dimension_id);
		printf(" Start: %lld ", (long long)start[dimension_id]);
		printf(" Block size : %lld ", (long long)block[dimension_id]);
		printf(" Stride size %lld ", (long long)stride[dimension_id]);
		uint64_t nelems = H5Sget_select_npoints(mem_space_id);
		printf("Number_of_elements: %zu\n", nelems);
		parh5D_print_data(buf, start[dimension_id], block[dimension_id], stride[dimension_id], nelems,
				  H5Tget_class(mem_type_id));
	}
}

herr_t parh5D_write(size_t count, void *dset[], hid_t mem_type_id[], hid_t mem_space_id[], hid_t file_space_id[],
		    hid_t dxpl_id, const void *buf[], void **req)
{
	(void)count;
	(void)dset;
	(void)mem_type_id;
	(void)mem_space_id;
	(void)file_space_id;
	(void)dxpl_id;
	(void)buf;
	(void)req;
	parh5D_print_datatypes(count, mem_type_id);
	for (size_t i = 0; i < count; i++)
		parh5D_print_memspace_info(mem_space_id[i], mem_type_id[i], buf[i]);

	log_fatal("Dataset: Sorry unimplemented function XXX TODO XXX");
	_exit(EXIT_FAILURE);
	return 1;
}

herr_t parh5D_get(void *obj, H5VL_dataset_get_args_t *args, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_fatal("Dataset: Sorry unimplemented function XXX TODO XXX");
	_exit(EXIT_FAILURE);
	return 1;
}

herr_t parh5D_specific(void *obj, H5VL_dataset_specific_args_t *args, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_fatal("Dataset: Sorry unimplemented function XXX TODO XXX");
	_exit(EXIT_FAILURE);
	return 1;
}

herr_t parh5D_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_fatal("Dataset: Sorry unimplemented function XXX TODO XXX");
	_exit(EXIT_FAILURE);
	return 1;
}

herr_t parh5D_close(void *dset, hid_t dxpl_id, void **req)
{
	(void)dset;
	(void)dxpl_id;
	(void)req;
	log_fatal("Dataset: Sorry unimplemented function XXX TODO XXX");
	_exit(EXIT_FAILURE);
	return 1;
}
