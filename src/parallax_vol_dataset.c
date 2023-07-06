#include "parallax_vol_dataset.h"
#include "H5Ppublic.h"
#include "H5Spublic.h"
#include "H5Tpublic.h"
#include "H5public.h"
#include "murmurhash.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_file.h"
#include "parallax_vol_group.h"
#include "parallax_vol_inode.h"
#include <H5Spublic.h>
#include <assert.h>
#include <log.h>
#include <parallax/parallax.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PARH5D_MAX_DIMENSIONS 5

#define PARH5D_PAR_CHECK_ERROR(X)                                 \
	if (X) {                                                  \
		log_fatal("Parallax reported this error: %s", X); \
		_exit(EXIT_FAILURE);                              \
	}

struct parh5D_uuid {
	uint64_t dset_id;
	uint64_t tile_id;
} __attribute((packed));

struct parh5D_tile {
	struct parh5D_uuid uuid;
	uint64_t offt_in_tile;
} __attribute((packed));

struct parh5D_dataset {
	H5I_type_t type;
	parh5I_inode_t inode;
	parh5F_file_t file; /*Where does this dataset belongs*/
	hid_t space_id; /*info about the space*/
	hid_t type_id; /*info about its schema*/
	hid_t dcpl_id; /*dataset creation property list*/
	uint32_t tile_size_in_elems;
};

/**
 * @brief Returns tile metadata to access a storage element.
 * @param dataset [in] pointer to the dataset object
 * @param storage_elem_id id of the storage element
 * @return a parh5D_tile object
 */
static struct parh5D_tile parh5D_map_id2tile(parh5D_dataset_t dataset, hsize_t storage_elem_id)
{
	struct parh5D_tile tile_uuid = {
		.uuid.dset_id = parh5I_get_inode_num(dataset->inode),
		.uuid.tile_id = storage_elem_id - storage_elem_id % dataset->tile_size_in_elems,
		.offt_in_tile = (storage_elem_id % dataset->tile_size_in_elems) * H5Tget_size(dataset->type_id)
	};
	return tile_uuid;
}

/**
 * @brief Calculates the final coordinates of an element id
 * @param [in] elem_id The id of the element in the memory buffer
 * @param [in] ndims number of dimensions of the storage array
 * @param [in] shape the dimensions of the storage array
 * @param [in] start the starting coordinates in the storage array
 * @param [in] end the bounds of coordinates in the storage array
 * @param [out] coords the coordinates that elem_id maps to
 * @return true on success false on failure
 */
static bool parh5D_get_coordinates(int elem_id, int ndims, hsize_t shape[], hsize_t start[], hsize_t end[],
				   hsize_t coords[])
{
	// Calculate the indices of the current element
	for (int dim_id = ndims - 1; dim_id >= 0; dim_id--) {
		// for (int dim_id = 0; dim_id < ndims; dim_id++) {
		coords[dim_id] = elem_id % shape[dim_id];
		// log_debug("elem id = %d shape[%i] = %ld coords[%d] = %ld", elem_id, dim_id, shape[dim_id], dim_id,
		// 	  coords[dim_id]);
		elem_id /= shape[dim_id];
		coords[dim_id] = start[dim_id] + coords[dim_id];
		if (coords[dim_id] > end[dim_id]) {
			log_debug("out of bounds for dim_id: %d coords[%d] = %ld end[%d] = %ld", dim_id, dim_id,
				  coords[dim_id], dim_id, end[dim_id]);

			log_debug("Translating elem_id was: %d, ndims: %d", elem_id, ndims);
			for (int i = 0; i < ndims; i++) {
				log_debug("Start[%d] = %ld", i, start[i]);
				log_debug("End[%d] = %ld", i, end[i]);
				log_debug("coords[%d] = %ld", i, coords[i]);
				log_debug("shape[%d] = %ld", i, shape[i]);
			}
			return false;
		}
	}
	return true;
}

/**
 * @brief Given a cell in a multi dimensional array returns its elem id
 * in 1D projection.
 * @param [in] coords array with the elements coordinates
 * @param [in] shape  array dimensions
 * @param [in] ndims number of dimensions
 * @return >=0 in case of SUCCESS otherwise -1 on failure
 */
static inline hsize_t parh5D_get_id_from_coords(hsize_t coords[], hsize_t shape[], int ndims)
{
	if (0 == ndims)
		return -1;

	int index = 0;
	int stride = 1;

	for (int i = ndims - 1; i >= 0; i--) {
		index += coords[i] * stride;
		stride *= shape[i];
	}
	return index;
}

/**
 * @brief Reads either a full or parts of a tile
 * @param [in] dataset reference to the dataset object
 * @param [in] tile metadata about the tile to read
 * @param [in] mem_buf to store part or the whole tile
 * @param [in] mem_buf_size size of the mem_buffer
 * @return number of bytes read
 */
static size_t parh5D_read_from_tile(parh5D_dataset_t dataset, struct parh5D_tile tile, char mem_buf[],
				    size_t mem_buf_size)
{
	size_t tile_size = dataset->tile_size_in_elems * H5Tget_size(dataset->type_id);
	bool subtile_access = tile.offt_in_tile || mem_buf_size < tile_size ? true : false;

	struct par_key par_key = { .size = sizeof(tile.uuid), .data = (char *)&tile.uuid };
	struct par_value par_value = { .val_buffer = mem_buf, .val_buffer_size = mem_buf_size };

	if (subtile_access) { //Misaligned access
		par_value.val_buffer = calloc(dataset->tile_size_in_elems, H5Tget_size(dataset->type_id));
		par_value.val_buffer_size = tile_size;
	}

	if (par_value.val_buffer_size < tile_size) {
		log_fatal("Buffer should be able to store one tile");
		_exit(EXIT_FAILURE);
	}

	const char *error = NULL;
	par_get(parh5F_get_parallax_db(dataset->file), &par_key, &par_value, &error);

	if (error) {
		log_debug("Tile with dset id: %lu of <dataset: %s>, tile_id: %lu does not exist reason: %s",
			  tile.uuid.dset_id, parh5I_get_inode_name(dataset->inode), tile.uuid.tile_id, error);
		return 0;
	}

	if (!subtile_access)
		return mem_buf_size;

	memcpy(mem_buf, &par_value.val_buffer[tile.offt_in_tile], mem_buf_size);
	free(par_value.val_buffer);
	return mem_buf_size;
}

/**
 * @brief Writes either a full or a partial tile
 * @param [in] dataset reference to dataset object
 * @param [in] tile contains metadata about the tile
 * @param [in] mem_buf pointer to the mem buf which contains the tile value
 * @param [in] mem_buf_size size of the mem_buf
 */
static size_t parh5D_write_tile(parh5D_dataset_t dataset, struct parh5D_tile tile, const char mem_buf[],
				size_t mem_buf_size)
{
	size_t tile_size = dataset->tile_size_in_elems * H5Tget_size(dataset->type_id);

	if (mem_buf_size > tile_size - tile.offt_in_tile) {
		log_fatal("buffer size exceeds tile boundaries mem_buf_size: %lu tile_size: %lu offt_in_tile: %lu",
			  mem_buf_size, tile_size, tile.offt_in_tile);
		_exit(EXIT_FAILURE);
	}

	bool subtile_access = tile.offt_in_tile || mem_buf_size < tile_size ? true : false;

	struct par_key par_key = { .size = sizeof(tile.uuid), .data = (char *)&tile.uuid };
	struct par_value par_value = { .val_buffer = (char *)mem_buf, .val_size = mem_buf_size };

	if (subtile_access) { //Misaligned access
		par_value.val_buffer = calloc(1UL, tile_size);
		par_value.val_buffer_size = tile_size;
		const char *error = NULL;
		par_get(parh5F_get_parallax_db(dataset->file), &par_key, &par_value, &error);
		// if (error)
		// 	log_debug("Ok tile not found don't worry it is normal reason: %s", error);
		memcpy(&par_value.val_buffer[tile.offt_in_tile], mem_buf, mem_buf_size);
	}

	assert(par_value.val_size);
	const char *error = NULL;
	struct par_key_value KV = { .k = par_key, .v = par_value };
	par_put(parh5F_get_parallax_db(dataset->file), &KV, &error);
	if (error) {
		log_fatal("Failed to write tile with dset_id: %lu tile_id: %lu reason: %s tile size is %lu",
			  tile.uuid.dset_id, tile.uuid.tile_id, error, tile_size);
		_exit(EXIT_FAILURE);
	}

	if (subtile_access)
		free(par_value.val_buffer);

	return mem_buf_size;
}

#define PAR5HD_BUFFER_CHECK_REMAINING(X, Y)                           \
	if (X < Y) {                                                  \
		log_fatal("Sorry need to resize inode XXX TODO XXX"); \
		_exit(EXIT_FAILURE);                                  \
	}

static bool parh5D_store_dataset(parh5D_dataset_t dset)
{
	size_t idx = 0;
	char *buffer = parh5I_get_inode_metadata_buf(dset->inode);
	size_t remaining_bytes = parh5I_get_inode_metadata_size();
	idx = 0;
	//Dataset's space_id
	size_t space_needed = 0;
	H5Sencode2(dset->space_id, NULL, &space_needed, 0);
	PAR5HD_BUFFER_CHECK_REMAINING(remaining_bytes, space_needed);

	if (H5Sencode2(dset->space_id, &buffer[idx], &space_needed, 0) < 0) {
		log_fatal("Failed tp encode");
		_exit(EXIT_FAILURE);
	}
	idx += space_needed;
	remaining_bytes -= space_needed;
	//Dataset's type_id
	H5Tencode(dset->type_id, NULL, &space_needed);
	if (0 == space_needed) {
		log_fatal("Failed to get the size of the type");
		_exit(EXIT_FAILURE);
	}
	PAR5HD_BUFFER_CHECK_REMAINING(remaining_bytes, space_needed);

	if (H5Tencode(dset->type_id, &buffer[idx], &space_needed) < 0) {
		log_fatal("Failed to serialize dataset type buffer");
		_exit(EXIT_FAILURE);
	}
	idx += space_needed;
	remaining_bytes -= space_needed;
	//Now the dataset creation property list
	H5Pencode1(dset->dcpl_id, NULL, &space_needed);
	log_debug("Space need for pl is %lu", space_needed);
	PAR5HD_BUFFER_CHECK_REMAINING(remaining_bytes, space_needed);
	if (H5Pencode1(dset->dcpl_id, &buffer[idx], &space_needed) < 0) {
		log_fatal("Failed to serialize dataset creation propery list");
		_exit(EXIT_FAILURE);
	}
	idx += space_needed;
	remaining_bytes -= space_needed;
	parh5I_store_inode(dset->inode, parh5F_get_parallax_db(dset->file));
	return true;
}

/**
 * @brief Deserializes the contents of a buffer into a dataspace object
 * @param [in] dataset pointer to the daset object which contents we fill
 * @param [in] buffer pointer to the buffer which contains the serialized data
 * @param [in] buffer_size the size of the buffer
 */
static void parh5D_deserialize_dataset(parh5D_dataset_t dataset)
{
	char *buffer = parh5I_get_inode_metadata_buf(dataset->inode);
	uint32_t idx = 0;
	//get the space id
	dataset->space_id = H5Sdecode(&buffer[idx]);
	if (dataset->space_id <= 0) {
		log_fatal("Failed to deserialize the space id of the dataset");
		_exit(EXIT_FAILURE);
	}
	size_t size = 0;
	H5Sencode2(dataset->space_id, NULL, &size, 0);
	idx += size;
	//Get the type id
	dataset->type_id = H5Tdecode(&buffer[idx]);
	if (dataset->type_id < 0) {
		log_fatal("Failed to decode dataset type");
		_exit(EXIT_FAILURE);
	}
	H5Tencode(dataset->type_id, NULL, &size);
	idx += size;
	//Get the dataset creation property list
	dataset->dcpl_id = H5Pdecode(&buffer[idx]);
}

parh5D_dataset_t parh5D_open_dataset(parh5I_inode_t inode, parh5F_file_t file)
{
	parh5D_dataset_t dataset = calloc(1UL, sizeof(*dataset));
	dataset->type = H5I_DATASET;
	dataset->inode = inode;
	dataset->file = file;
	parh5D_deserialize_dataset(dataset);
	return dataset;
}

static parh5D_dataset_t parh5D_read_dataset(parh5G_group_t group, uint64_t inode_num)
{
	parh5I_inode_t inode = parh5I_get_inode(parh5G_get_parallax_db(group), inode_num);
	parh5D_dataset_t dataset = calloc(1UL, sizeof(*dataset));
	dataset->type = H5I_DATASET;
	dataset->file = parh5G_get_file(group);
	dataset->inode = inode;
	parh5D_deserialize_dataset(dataset);
	return dataset;
}

static void parh5D_set_tile_size(parh5D_dataset_t dataset)
{
	// Get the class of the datatype
	H5T_class_t class_id = H5Tget_class(dataset->type_id);
	dataset->tile_size_in_elems = class_id == H5T_FLOAT || class_id == H5T_INTEGER ? 32 : 1;
}

void *parh5D_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id,
		    hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
	(void)loc_params;
	(void)name;
	(void)lcpl_id;
	(void)dcpl_id;
	(void)dapl_id;
	(void)req;
	(void)dxpl_id;
	H5I_type_t *obj_type = obj;

	parh5G_group_t parent_group = NULL;

	if (H5I_GROUP == *obj_type)
		parent_group = (parh5G_group_t)obj;
	else if (H5I_FILE == *obj_type) {
		parh5F_file_t file = (parh5F_file_t)obj;
		parent_group = parh5F_get_root_group(file);
	} else {
		log_fatal("Dataset can only be associated with a group/file object");
		_exit(EXIT_FAILURE);
	}

	parh5D_dataset_t dataset = calloc(1UL, sizeof(struct parh5D_dataset));
	dataset->type = H5I_DATASET;

	dataset->inode = parh5I_create_inode(name, H5I_DATASET, parh5G_get_root_inode(parent_group),
					     parh5G_get_parallax_db(parent_group));
	log_debug("Creating dataspace in parent group %s", parh5I_get_inode_name(parh5G_get_inode(parent_group)));
	dataset->type_id = H5Tcopy(type_id);
	dataset->dcpl_id = H5Pcopy(dcpl_id);
	dataset->file = parh5G_get_file(parent_group);
	dataset->space_id = H5Scopy(space_id);
	parh5D_store_dataset(dataset);
	parh5I_add_pivot_in_inode(parh5G_get_inode(parent_group), parh5I_get_inode_num(dataset->inode), name,
				  parh5G_get_parallax_db(parent_group));
	parh5I_store_inode(parh5G_get_inode(parent_group), parh5G_get_parallax_db(parent_group));

	parh5D_set_tile_size(dataset);
	log_debug("Dimensions of new dataspace are %d", H5Sget_simple_extent_ndims(dataset->space_id));

	return dataset;
}

void *parh5D_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id,
		  void **req)
{
	(void)loc_params;
	(void)dapl_id;
	(void)dxpl_id;
	(void)req;
	H5I_type_t *obj_type = obj;

	parh5G_group_t parent_group = NULL;

	if (H5I_GROUP == *obj_type)
		parent_group = (parh5G_group_t)obj;
	else if (H5I_FILE == *obj_type) {
		parh5F_file_t file = (parh5F_file_t)obj;
		parent_group = parh5F_get_root_group(file);
	} else {
		log_fatal("Dataset can only be associated with a group/file object");
		_exit(EXIT_FAILURE);
	}
	uint64_t inode_num =
		parh5I_path_search(parh5G_get_inode(parent_group), name, parh5G_get_parallax_db(parent_group));
	if (0 == inode_num) {
		log_debug("Dataset: %s does not exist", name);
		return NULL;
	}

	parh5D_dataset_t dataset = parh5D_read_dataset(parent_group, inode_num);
	parh5D_set_tile_size(dataset);
	assert(dataset);
	return dataset;
}

herr_t parh5D_read(size_t count, void *dset[], hid_t mem_type_id[], hid_t mem_space_id[], hid_t file_space_id[],
		   hid_t dxpl_id, void *buf[], void **req)
{
	(void)dxpl_id;
	(void)req;
	if (0 == count) {
		log_warn("Zero operations defined?");
		return PARH5_SUCCESS;
	}

	if (count > 1) {
		log_fatal("Sorry cannot support yet scatter/gather write I/Os");
		_exit(EXIT_FAILURE);
	}

	H5I_type_t *obj_type = dset[0];
	if (H5I_DATASET != *obj_type) {
		log_fatal("Dataset write can only be associated with a file object");
		_exit(EXIT_FAILURE);
	}

	parh5D_dataset_t dataset = (parh5D_dataset_t)dset[0];
	assert(H5Tequal(mem_type_id[0], dataset->type_id));

	hid_t real_file_space_id = file_space_id[0] == H5S_ALL ? dataset->space_id : file_space_id[0];
	hid_t real_mem_space_id = mem_space_id[0] == H5S_ALL ? dataset->space_id : mem_space_id[0];

	/* Get number of elements in selections */
	hssize_t num_elem_file = -1;
	if ((num_elem_file = H5Sget_select_npoints(real_file_space_id)) < 0) {
		log_fatal("can't get number of points in file selection");
		_exit(EXIT_FAILURE);
	}
	hssize_t num_elem_mem = -1;
	if ((num_elem_mem = H5Sget_select_npoints(real_mem_space_id)) < 0) {
		log_fatal("can't get number of points in memory selection");
		_exit(EXIT_FAILURE);
	}

	/* Various sanity and special case checks */
	if (num_elem_file != num_elem_mem) {
		log_fatal("number of elements selected in file and memory dataspaces is different");
		_exit(EXIT_FAILURE);
	}

	if (num_elem_file && !buf) {
		log_fatal("write buffer is NULL but selection has >0 elements");
		_exit(EXIT_FAILURE);
	}

	if (num_elem_file == 0)
		return PARH5_SUCCESS;

	//First let's translate the 1D memory element id into its n coordinates
	hsize_t start_coords[PARH5D_MAX_DIMENSIONS];
	hsize_t end_coords[PARH5D_MAX_DIMENSIONS];
	if (H5Sget_select_bounds(real_file_space_id, start_coords, end_coords) < 0) {
		log_fatal("Failed to get start, end bounds");
		_exit(EXIT_FAILURE);
	}

	hsize_t file_shape[PARH5D_MAX_DIMENSIONS] = { 0 };
	int file_ndims = H5Sget_simple_extent_dims(real_file_space_id, file_shape, NULL);
	if (file_ndims < 0) {
		log_fatal("Failed to get file dimensions");
		_exit(EXIT_FAILURE);
	}

	hsize_t tile_size = dataset->tile_size_in_elems * H5Tget_size(dataset->type_id);
	size_t mem_buf_size = num_elem_mem * H5Tget_size(mem_type_id[0]);
	char *mem_buf = buf[0];
	size_t total_bytes_read = 0;
	for (hssize_t mem_elem_id = 0; mem_elem_id < num_elem_mem;) {
		//1st translation: Translate mem buffer id to storage elem id. Two steps process
		//a) Translate id to n coordinates of the storage array
		hsize_t coords[PARH5D_MAX_DIMENSIONS] = { 0 };
		parh5D_get_coordinates(mem_elem_id, file_ndims, file_shape, start_coords, end_coords, coords);
		// log_debug("Translating mem_elem_id %ld to n coordinates:", mem_elem_id);
		// for (int i = 0; i < file_ndims; i++)
		// 	log_debug("coords[%d]=%ld", i, coords[i]);

		//b) Translate n coordinates to storage element id
		hsize_t storage_elem_id = parh5D_get_id_from_coords(coords, file_shape, file_ndims);
		// log_debug("Dataset: %s mem elem id: %ld maps to storage_id %ld", parh5I_get_inode_name(dataset->inode),
		// 	  mem_elem_id, storage_elem_id);

		//2nd translation: Translate storage_elem_id to tile uuid and off it tile
		struct parh5D_tile tile = parh5D_map_id2tile(dataset, storage_elem_id);
		// log_debug("Dataset: %s storage_elem_id %ld maps to tile <dset_id: %ld tile_id: %ld offt_in_tile: %ld>",
		// 	  parh5I_get_inode_name(dataset->inode), storage_elem_id, tile.uuid.dset_id, tile.uuid.tile_id,
		// 	  tile.offt_in_tile);
		size_t bytes_to_read = tile.offt_in_tile ? tile_size - tile.offt_in_tile : tile_size;
		if (bytes_to_read > mem_buf_size - total_bytes_read)
			bytes_to_read = mem_buf_size - total_bytes_read;

		size_t bytes_read = parh5D_read_from_tile(dataset, tile, &mem_buf[total_bytes_read], bytes_to_read);
		assert(bytes_read == bytes_to_read);
		total_bytes_read += bytes_read;
		mem_elem_id += bytes_read / H5Tget_size(dataset->type_id);
	}

	return PARH5_SUCCESS;
}

// static void parh5D_print_value(const void *data, int idx, H5T_class_t data_type)
// {
// 	switch (data_type) {
// 	case H5T_INTEGER: {
// 		int *data_t = (int *)data;
// 		printf("[%d] = %d, ", idx, data_t[idx]);
// 	} break;
// 	case H5T_FLOAT: {
// 		float *data_t = (float *)data;
// 		printf("[%d] = %f, ", idx, data_t[idx]);
// 	} break;
// 	case H5T_STRING: {
// 		char **data_t = (char **)data;
// 		printf("[%d] = %s, ", idx, data_t[idx]);
// 	} break;
// 	default:
// 		log_fatal("Memory Datatype[%d]: Unknown Brr", data_type);
// 		assert(0);
// 		_exit(EXIT_FAILURE);
// 	}
// }

// static void parh5D_print_data(const void *data, hsize_t start, hsize_t block, hsize_t stride, hsize_t nelems,
// 			      H5T_class_t data_type)
// {
// 	log_debug("Start %zu block %zu stride %zu nelems %zu", start, block, stride, nelems);
// 	hsize_t array_id = start;
// 	while (array_id < start + nelems) {
// 		// Process a block of items
// 		for (hsize_t elems_cnt = 0; elems_cnt < block && array_id < start + nelems; elems_cnt++, array_id++) {
// 			parh5D_print_value(data, array_id, data_type);
// 		}

// 		// Skip the specified number of elements
// 		array_id += stride - 1;
// 	}
// 	printf("\n");
// }

// static void parh5D_print_space_info(hid_t space_id, hid_t data_type_id, const void *buf)
// {
// 	int ndimensions = H5Sget_simple_extent_ndims(space_id);
// 	if (ndimensions > PARH5D_MAX_DIMENSIONS) {
// 		log_fatal("Sorry cannot print more than %d DIMS", PARH5D_MAX_DIMENSIONS);
// 		_exit(EXIT_FAILURE);
// 	}
// 	hsize_t start[PARH5D_MAX_DIMENSIONS] = { 0 };
// 	hsize_t block[PARH5D_MAX_DIMENSIONS] = { 0 };
// 	hsize_t stride[PARH5D_MAX_DIMENSIONS] = { 0 };
// 	hsize_t count[PARH5D_MAX_DIMENSIONS] = { 0 };
// 	hsize_t dimension_size[PARH5D_MAX_DIMENSIONS] = { 0 };
// 	H5Sget_simple_extent_dims(space_id, dimension_size, NULL);

// 	/* Print memory dataspace start index, block, and stride */
// 	uint64_t total_elems = H5Sget_select_npoints(space_id);
// 	log_debug("Info for space of total dimensions %d total elements %zu", ndimensions, total_elems);
// 	H5Sget_regular_hyperslab(space_id, start, stride, count, block);

// 	// Print the size of each dimension
// 	for (int dimension_id = 0; dimension_id < ndimensions; dimension_id++) {
// 		printf(" Dimension %d ", dimension_id);
// 		printf(" Start: %zu ", start[dimension_id]);
// 		printf(" Block size : %zu ", block[dimension_id]);
// 		printf(" Stride size %zu ", stride[dimension_id]);
// 		printf(" Elements in dimesion %d = %zu\n", dimension_id, dimension_size[dimension_id]);
// 		if (NULL == buf)
// 			continue;
// 		parh5D_print_data(buf, start[dimension_id], block[dimension_id], stride[dimension_id], total_elems,
// 				  H5Tget_class(data_type_id));
// 	}
// }

herr_t parh5D_write(size_t count, void *dset[], hid_t mem_type_id[], hid_t mem_space_id[], hid_t file_space_id[],
		    hid_t dxpl_id, const void *buf[], void **req)
{
	(void)req;
	(void)dxpl_id;

	if (0 == count) {
		log_warn("Zero operations defined?");
		return PARH5_SUCCESS;
	}

	if (count > 1) {
		log_fatal("Sorry cannot support yet scatter/gather write I/Os");
		_exit(EXIT_FAILURE);
	}

	H5I_type_t *obj_type = dset[0];
	if (H5I_DATASET != *obj_type) {
		log_fatal("Dataset write can only be associated with a file object");
		_exit(EXIT_FAILURE);
	}

	parh5D_dataset_t dataset = (parh5D_dataset_t)dset[0];
	if (!H5Tequal(mem_type_id[0], dataset->type_id)) {
		log_fatal("Sorry Parallax does not support dynamic types yet");
		_exit(EXIT_FAILURE);
	}

	/* Get dataspace extent */
	int dpace_ndims = 0;
	if ((dpace_ndims = H5Sget_simple_extent_ndims(dataset->space_id)) < 0) {
		log_fatal("can't get number of dimensions");
		_exit(EXIT_FAILURE);
	}

	hid_t real_file_space_id = file_space_id[0] == H5S_ALL ? dataset->space_id : file_space_id[0];
	hid_t real_mem_space_id = mem_space_id[0] == H5S_ALL ? dataset->space_id : mem_space_id[0];

	if (H5Sget_simple_extent_ndims(real_file_space_id) != H5Sget_simple_extent_ndims(real_mem_space_id)) {
		log_debug("Dimensions between memory and file differ");
		_exit(EXIT_FAILURE);
	}

	hssize_t num_elem_mem = -1;
	if ((num_elem_mem = H5Sget_select_npoints(real_mem_space_id)) < 0) {
		log_fatal("can't get number of points in memory selection");
		assert(0);
		_exit(EXIT_FAILURE);
	}

	/* Get number of elements in selections */
	hssize_t num_elem_file = -1;
	if ((num_elem_file = H5Sget_select_npoints(real_file_space_id)) < 0) {
		log_fatal("can't get number of points in file selection for dataset: %s",
			  parh5I_get_inode_name(dataset->inode));
		_exit(EXIT_FAILURE);
	}

	/* Various sanity and special case checks */
	if (num_elem_file != num_elem_mem)
		log_fatal("number of elements selected in file and memory dataspaces is different");

	if (num_elem_file && !buf)
		log_fatal("write buffer is NULL but selection has >0 elements");

	if (num_elem_file == 0)
		return PARH5_SUCCESS;

	//First let's translate the 1D memory element id into its n coordinates
	hsize_t start_coords[PARH5D_MAX_DIMENSIONS];
	hsize_t end_coords[PARH5D_MAX_DIMENSIONS];
	if (H5Sget_select_bounds(real_file_space_id, start_coords, end_coords) < 0) {
		log_fatal("Failed to get start, end bounds");
		_exit(EXIT_FAILURE);
	}

	hsize_t file_shape[PARH5D_MAX_DIMENSIONS] = { 0 };
	int file_ndims = H5Sget_simple_extent_dims(real_file_space_id, file_shape, NULL);
	if (file_ndims < 0) {
		log_fatal("Failed to get file dimensions");
		_exit(EXIT_FAILURE);
	}

	hsize_t tile_size = dataset->tile_size_in_elems * H5Tget_size(dataset->type_id);
	size_t mem_buf_size = num_elem_mem * H5Tget_size(dataset->type_id);
	const char *mem_buf = buf[0];
	size_t total_bytes_written = 0;
	for (hssize_t mem_elem_id = 0; mem_elem_id < num_elem_mem;) {
		//1st translation: Translate mem buffer id to storage elem id. Two steps process
		//a) Translate id to n coordinates of the storage array
		hsize_t coords[PARH5D_MAX_DIMENSIONS];
		parh5D_get_coordinates(mem_elem_id, file_ndims, file_shape, start_coords, end_coords, coords);

		//b) Translate n coordinates to storage element id
		hsize_t storage_elem_id = parh5D_get_id_from_coords(coords, file_shape, file_ndims);
		// log_debug("Dataset: %s mem elem id: %ld maps to storage_id %ld", parh5I_get_inode_name(dataset->inode),
		// 	  mem_elem_id, storage_elem_id);

		//2nd translation: Translate storage_elem_id to tile uuid and off it tile
		struct parh5D_tile tile = parh5D_map_id2tile(dataset, storage_elem_id);
		// log_debug("Dataset: %s storage_elem_id %ld maps to tile <dset_id: %ld tile_id: %ld offt_in_tile: %ld>",
		// 	  parh5I_get_inode_name(dataset->inode), storage_elem_id, tile.uuid.dset_id, tile.uuid.tile_id,
		// 	  tile.offt_in_tile);

		size_t bytes_to_write = tile.offt_in_tile ? tile_size - tile.offt_in_tile : tile_size;
		if (bytes_to_write > mem_buf_size - total_bytes_written)
			bytes_to_write = mem_buf_size - total_bytes_written;

		size_t bytes_written = parh5D_write_tile(dataset, tile, &mem_buf[total_bytes_written], bytes_to_write);
		assert(bytes_written == bytes_to_write);
		total_bytes_written += bytes_written;
		// log_debug("Elements written are %lu bytes_written are: %lu type size: %lu",
		// 	  bytes_written / H5Tget_size(dataset->type_id), bytes_written, H5Tget_size(dataset->type_id));
		mem_elem_id += bytes_written / H5Tget_size(dataset->type_id);
		// log_debug("mem_elem_id = %ld total_bytes_written: %lu total elements to write: %lu", mem_elem_id,
		// 	  total_bytes_written, num_elem_mem);
	}

	return PARH5_SUCCESS;
}

herr_t parh5D_get(void *obj, H5VL_dataset_get_args_t *get_op, hid_t dxpl_id, void **req)
{
	(void)dxpl_id;
	(void)req;
	H5I_type_t *obj_type = obj;
	if (H5I_DATASET != *obj_type) {
		log_fatal("Object is not a dataset!");
		_exit(EXIT_FAILURE);
	}
	parh5D_dataset_t dataset = obj;

	switch (get_op->op_type) {
	case H5VL_DATASET_GET_SPACE:
		// log_debug("HDF5 wants to know about space of dataset: %s nlinks are: %u",
		// 	  parh5I_get_inode_name(dataset->inode), parh5I_get_nlinks(dataset->inode));
		get_op->args.get_space.space_id = H5Scopy(dataset->space_id);
		if (get_op->args.get_space.space_id < 0) {
			log_fatal("Failed to copy space");
			_exit(EXIT_FAILURE);
		}
		break;
	case H5VL_DATASET_GET_TYPE:
		get_op->args.get_type.type_id = dataset->type_id;
		break;
	case H5VL_DATASET_GET_DCPL:
		get_op->args.get_dcpl.dcpl_id = dataset->dcpl_id;
		break;
	default:
		log_debug("Sorry HDF5 cannot answer %d yet", get_op->op_type);
		assert(0);
		_exit(EXIT_FAILURE);
		break;
	}

	return PARH5_SUCCESS;
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
	(void)dxpl_id;
	(void)req;

	H5I_type_t *obj_type = dset;
	if (H5I_DATASET != *obj_type) {
		log_fatal("Dataset write can only be associated with a file object");
		_exit(EXIT_FAILURE);
	}

	parh5D_dataset_t dataset = dset;
	//Don't worry about space, type, and dcpl. HDF5 knows about their existence
	//since it has asked the plugin during open and cleans them up itself

	// log_debug("Closing dataset %s SUCCESS", parh5I_get_inode_name(dataset->inode));
	free(dataset->inode);
	free(dataset);

	return PARH5_SUCCESS;
}

const char *parh5D_get_dataset_name(parh5D_dataset_t dataset)
{
	return dataset ? parh5I_get_inode_name(dataset->inode) : NULL;
}

parh5I_inode_t parh5D_get_inode(parh5D_dataset_t dataset)
{
	return dataset ? dataset->inode : NULL;
}

parh5F_file_t parh5D_get_file(parh5D_dataset_t dataset)
{
	return dataset ? dataset->file : NULL;
}
