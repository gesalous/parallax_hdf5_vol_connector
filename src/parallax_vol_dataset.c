#include "parallax_vol_dataset.h"
#include "H5Ppublic.h"
#include "H5Spublic.h"
#include "H5Tpublic.h"
#include "H5public.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_file.h"
#include "parallax_vol_group.h"
#include "parallax_vol_inode.h"
#include "parallax_vol_tile_cache.h"
#include <H5Spublic.h>
#include <assert.h>
#include <log.h>
#include <parallax/parallax.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#ifdef METRICS_ENABLE
#include "parallax_vol_metrics.h"
#endif

#define PARH5D_MAX_DIMENSIONS 5
#define PARH5D_CONTIGUOUS_TILE_SIZE 512
#define PARH5D_CHUNKED_TILE_SIZE 64

#define PARH5D_PAR_CHECK_ERROR(X)                                 \
	if (X) {                                                  \
		log_fatal("Parallax reported this error: %s", X); \
		_exit(EXIT_FAILURE);                              \
	}

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
static struct parh5T_tile parh5D_map_id2tile(parh5D_dataset_t dataset, hsize_t storage_elem_id)
{
	struct parh5T_tile tile_uuid = {
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
// static bool parh5D_get_coordinates(int elem_id, int ndims, hsize_t shape[], hsize_t start[], hsize_t end[],
// 				   hsize_t coords[])
// {
// 	// Calculate the indices of the current element
// 	for (int dim_id = ndims - 1; dim_id >= 0; dim_id--) {
// 		// for (int dim_id = 0; dim_id < ndims; dim_id++) {
// 		coords[dim_id] = elem_id % shape[dim_id];
// 		// log_debug("elem id = %d shape[%i] = %ld coords[%d] = %ld", elem_id, dim_id, shape[dim_id], dim_id,
// 		// 	  coords[dim_id]);
// 		elem_id /= shape[dim_id];
// 		coords[dim_id] = start[dim_id] + coords[dim_id];
// 		if (coords[dim_id] > end[dim_id]) {
// 			log_debug("out of bounds for dim_id: %d coords[%d] = %ld end[%d] = %ld", dim_id, dim_id,
// 				  coords[dim_id], dim_id, end[dim_id]);

// 			log_debug("Translating elem_id was: %d, ndims: %d", elem_id, ndims);
// 			for (int i = 0; i < ndims; i++) {
// 				log_debug("Start[%d] = %ld", i, start[i]);
// 				log_debug("End[%d] = %ld", i, end[i]);
// 				log_debug("coords[%d] = %ld", i, coords[i]);
// 				log_debug("shape[%d] = %ld", i, shape[i]);
// 			}
// 			return false;
// 		}
// 	}
// 	return true;
// }

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

	for (int i = 0; i < ndims; i++) {
		index += coords[i] * stride;
		stride *= shape[i];
	}

	// for (int i = ndims - 1; i >= 0; i--)
	// 	fprintf(stderr, "storage_coord[%d] = %ld ", i, coords[i]);
	// fprintf(stderr, "\n");
	// fprintf(stderr, "Element id (in 1D) = %d\n", index);

	return index;
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
#ifdef METRICS_ENABLE
	parh5M_inc_dset_metadata_bytes_written(dset, parh5I_get_inode_size());
#endif
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
	parh5D_dataset_t dset = calloc(1UL, sizeof(*dset));
	dset->type = H5I_DATASET;
	dset->inode = inode;
	dset->file = file;
	parh5D_deserialize_dataset(dset);
	return dset;
}

static parh5D_dataset_t parh5D_read_dataset(parh5G_group_t group, uint64_t inode_num)
{
	parh5I_inode_t inode = parh5I_get_inode(parh5G_get_parallax_db(group), inode_num);
	parh5D_dataset_t dset = calloc(1UL, sizeof(*dset));
	dset->type = H5I_DATASET;
	dset->file = parh5G_get_file(group);
	dset->inode = inode;
	parh5D_deserialize_dataset(dset);
#ifdef METRICS_ENABLE
	parh5M_inc_dset_metadata_bytes_read(dset, parh5I_get_inode_size());
#endif
	return dset;
}

static void parh5D_set_tile_size(parh5D_dataset_t dataset)
{
	H5D_layout_t layout = H5Pget_layout(dataset->dcpl_id);
	H5T_class_t class_id = H5Tget_class(dataset->type_id);

	dataset->tile_size_in_elems = PARH5D_CHUNKED_TILE_SIZE;
	if (layout == H5D_CONTIGUOUS)
		dataset->tile_size_in_elems = PARH5D_CONTIGUOUS_TILE_SIZE;

	if (class_id != H5T_FLOAT && class_id != H5T_INTEGER && class_id != H5T_NATIVE_DOUBLE)
		dataset->tile_size_in_elems = 1;

	hsize_t dims[PARH5D_MAX_DIMENSIONS] = { 0 };
	H5Sget_simple_extent_dims(dataset->space_id, dims, NULL);
	if (dataset->tile_size_in_elems > dims[0])
		dataset->tile_size_in_elems = dims[0];

	fprintf(stderr, "Set tile size in elements %u\n", dataset->tile_size_in_elems);
}

void *parh5D_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id,
		    hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
	(void)loc_params;
	(void)name;
	(void)lcpl_id;
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

	// Get the storage layout from the dataset creation property list
	// H5D_layout_t layout = H5Pget_layout(dcpl_id);

	// Print the storage layout
	// if (layout == H5D_CHUNKED)
	// 	fprintf(stderr, "Storage Layout: Chunked for dataset: %s\n", name);
	// else if (layout == H5D_CONTIGUOUS)
	// 	fprintf(stderr, "Storage Layout: Contiguous for dataset: %s\n", name);
	// else if (layout == H5D_COMPACT)
	// 	fprintf(stderr, "Storage Layout: Compact for dataset %s\n", name);
	// else
	// 	fprintf(stderr, "Storage Layout: Unknown\n");

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

/**
 * @brief Based on the start array returns the coordinates of the first element.
 * @param ndims [in] number of array dimensions.
 * @param coordinates [out] fills the coordinates of the the first element.
 * @param start [in] the starting coordinates of the element.
 */
static void parh5D_get_first_array_element(int ndims, hsize_t coordinates[], hsize_t start[])
{
	// if (ndims > 2)
	// 	fprintf(stderr, "*--*\n");

	for (int dim = 0; dim < ndims; dim++) {
		coordinates[dim] = start[dim];
		// if (ndims > 2)
		fprintf(stderr, "Fresh start[%d]=%ld\n", dim, start[dim]);
	}
	// if (ndims > 2)
	// 	fprintf(stderr, "*--*\n");
}

/**
 * @brief Returns the coordinates of the next element in row-wise iteration.
 * @param ndims [in] number of array dimensions
 * @param coordinates [out] the coordinates of the next element
 * @param start [in] the starting coordinates of the selection
 * @param end [in] the ending coordinates of the selection
 */
static void parh5D_get_next_array_element(int ndims, hsize_t coordinates[], hsize_t start[], hsize_t end[])
{
	// if (ndims > 2) {
	// 	fprintf(stderr, "**\n");
	// 	fprintf(stderr, "Before\n");
	// }
	// for (int id = ndims - 1; id >= 0 && ndims > 2; id--) {
	// fprintf(stderr, "start[%d] = %ld\n", id, start[id]);
	// fprintf(stderr, "end[%d] = %ld\n", id, end[id]);
	// fprintf(stderr, "coord[%d] = %ld\n", id, coordinates[id]);
	// }

	int i = 0;
	while (i < ndims) {
		if (coordinates[i] < end[i]) {
			coordinates[i]++;
			break;
		}
		coordinates[i] = start[i];
		i++;
	}
	// if (ndims > 2) {
	// 	fprintf(stderr, "**\n");
	// 	fprintf(stderr, "After\n");
	// }
	for (int id = ndims - 1; id >= 0 && ndims > 2; id--) {
		fprintf(stderr, "start[%d] = %ld\n", id, start[id]);
		fprintf(stderr, "end[%d] = %ld\n", id, end[id]);
		fprintf(stderr, "coord[%d] = %ld\n", id, coordinates[id]);
	}
}

/**
 * @brief Calculates the address of the element in a N-dimensional array.
 * @param  base_addr [in] starting address of the array in memory
 * @param ndims [in] dimensions of the in memory array
 * @param dims [in] the actual dimensions of the in memory array
 * @param coords [in] the coordinates of the element
 * @param elem_size [in] the size in bytes of each cell of the array
 * @return a pointer to the element
 */
static char *parh5D_calc_elem_addr(const char *base_addr, int ndims, size_t dims[], size_t coords[], size_t elem_size)
{
	// log_debug("Ndims are: %d", ndims);
	// for (int i = 0; i < ndims; i++) {
	// 	fprintf(stderr, "element[%d] = %ld\n", i, coords[i]);
	// 	fprintf(stderr, "dimension[%d] = %ld\n", i, dims[i]);
	// }
	char *element_addr = (char *)base_addr;

	size_t stride = elem_size;

	for (int i = 0; i < ndims; i++) {
		element_addr += (coords[i] * stride);
		stride *= dims[i];
	}

	return element_addr;
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
		log_fatal("buffer is NULL but selection has >0 elements");
		_exit(EXIT_FAILURE);
	}

	if (num_elem_file == 0)
		return PARH5_SUCCESS;

	if (num_elem_file == 0)
		return PARH5_SUCCESS;

	//Retrieve mem ndims and start, end coords
	hsize_t mem_shape[PARH5D_MAX_DIMENSIONS] = { 0 };
	int mem_ndims = H5Sget_simple_extent_dims(real_mem_space_id, mem_shape, NULL);
	if (mem_ndims < 0) {
		log_fatal("Failed to get file dimensions");
		_exit(EXIT_FAILURE);
	}
	hsize_t mem_start_coords[PARH5D_MAX_DIMENSIONS];
	hsize_t mem_end_coords[PARH5D_MAX_DIMENSIONS];
	if (H5Sget_select_bounds(real_mem_space_id, mem_start_coords, mem_end_coords) < 0) {
		log_fatal("Failed to get start, end bounds");
		_exit(EXIT_FAILURE);
	}
	//Retrieve file ndims and start, end coords
	hsize_t file_shape[PARH5D_MAX_DIMENSIONS] = { 0 };
	int file_ndims = H5Sget_simple_extent_dims(real_file_space_id, file_shape, NULL);
	if (file_ndims < 0) {
		log_fatal("Failed to get file dimensions");
		_exit(EXIT_FAILURE);
	}
	hsize_t file_start_coords[PARH5D_MAX_DIMENSIONS];
	hsize_t file_end_coords[PARH5D_MAX_DIMENSIONS];
	if (H5Sget_select_bounds(real_file_space_id, file_start_coords, file_end_coords) < 0) {
		log_fatal("Failed to get start, end bounds");
		_exit(EXIT_FAILURE);
	}

	size_t mem_buf_size = num_elem_mem * H5Tget_size(dataset->type_id);

	hsize_t mem_coords[PARH5D_MAX_DIMENSIONS] = { 0 };
	parh5D_get_first_array_element(mem_ndims, mem_coords, mem_start_coords);

	hsize_t file_coords[PARH5D_MAX_DIMENSIONS] = { 0 };
	parh5D_get_first_array_element(file_ndims, file_coords, file_start_coords);

#ifdef METRICS_ENABLE
	parh5M_inc_dset_bytes_read(dataset, mem_buf_size);
#endif

	parh5T_tile_cache_t tile_cache = parh5T_init_tile_cache(dataset, PARH5D_READ_TILE_CACHE);
	const char *mem_buf = buf[0];

	for (hssize_t elem_num = 0; elem_num < num_elem_mem; elem_num++) {
		char *elem_addr =
			parh5D_calc_elem_addr(mem_buf, mem_ndims, mem_shape, mem_coords, H5Tget_size(dataset->type_id));

		hsize_t file_elem_id = parh5D_get_id_from_coords(file_coords, file_shape, file_ndims);
		struct parh5T_tile file_tile = parh5D_map_id2tile(dataset, file_elem_id);
		parh5T_read_from_tile_cache(tile_cache, file_tile, elem_addr, H5Tget_size(dataset->type_id));

		parh5D_get_next_array_element(mem_ndims, mem_coords, mem_start_coords, mem_end_coords);
		parh5D_get_next_array_element(file_ndims, file_coords, file_start_coords, file_end_coords);
	}
	parh5T_destroy_tile_cache(tile_cache);

	return PARH5_SUCCESS;
}

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

	// if (H5Sget_simple_extent_ndims(real_file_space_id) != H5Sget_simple_extent_ndims(real_mem_space_id)) {
	// 	log_debug("Dimensions between memory and file differ");
	// 	int mem_ndims = H5Sget_simple_extent_ndims(real_mem_space_id);
	// 	log_debug("Memory dimensions: %d", mem_ndims);
	// 	log_debug("Mem n points: %ld", H5Sget_select_npoints(real_mem_space_id));
	// 	log_debug("Memory shape:");
	// 	hsize_t memory_shape[PARH5D_MAX_DIMENSIONS] = { 0 };
	// 	H5Sget_simple_extent_dims(real_mem_space_id, memory_shape, NULL);
	// 	for (int i = 0; i < mem_ndims; i++)
	// 		fprintf(stderr, "memory[%d] = %ld\n", i, memory_shape[i]);
	// 	//
	// 	int file_ndims = H5Sget_simple_extent_ndims(real_file_space_id);
	// 	log_debug("File dimensions: %d", file_ndims);
	// 	log_debug("File n points: %ld", H5Sget_select_npoints(real_file_space_id));
	// 	log_debug("File shape:");
	// 	hsize_t file_shape[PARH5D_MAX_DIMENSIONS] = { 0 };
	// 	H5Sget_simple_extent_dims(real_file_space_id, file_shape, NULL);
	// 	for (int i = 0; i < file_ndims; i++)
	// 		fprintf(stderr, "file[%d] = %ld\n", i, file_shape[i]);

	// 	hsize_t start_coords[PARH5D_MAX_DIMENSIONS];
	// 	hsize_t end_coords[PARH5D_MAX_DIMENSIONS];
	// 	if (H5Sget_select_bounds(real_file_space_id, start_coords, end_coords) < 0) {
	// 		log_fatal("Failed to get start, end bounds");
	// 		_exit(EXIT_FAILURE);
	// 	}
	// 	for (int i = 0; i < file_ndims; i++) {
	// 		fprintf(stdout, "start_coord[%d] = %ld\n", i, start_coords[i]);
	// 		fprintf(stdout, "end_coord[%d] = %ld\n", i, end_coords[i]);
	// 	}

	// 	_exit(EXIT_FAILURE);
	// }

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

	//Retrieve mem ndims and start, end coords
	hsize_t mem_shape[PARH5D_MAX_DIMENSIONS] = { 0 };
	int mem_ndims = H5Sget_simple_extent_dims(real_mem_space_id, mem_shape, NULL);
	if (mem_ndims < 0) {
		log_fatal("Failed to get file dimensions");
		_exit(EXIT_FAILURE);
	}
	hsize_t mem_start_coords[PARH5D_MAX_DIMENSIONS];
	hsize_t mem_end_coords[PARH5D_MAX_DIMENSIONS];
	if (H5Sget_select_bounds(real_mem_space_id, mem_start_coords, mem_end_coords) < 0) {
		log_fatal("Failed to get start, end bounds");
		_exit(EXIT_FAILURE);
	}
	//Retrieve file ndims and start, end coords
	hsize_t file_shape[PARH5D_MAX_DIMENSIONS] = { 0 };
	int file_ndims = H5Sget_simple_extent_dims(real_file_space_id, file_shape, NULL);
	if (file_ndims < 0) {
		log_fatal("Failed to get file dimensions");
		_exit(EXIT_FAILURE);
	}
	hsize_t file_start_coords[PARH5D_MAX_DIMENSIONS];
	hsize_t file_end_coords[PARH5D_MAX_DIMENSIONS];
	if (H5Sget_select_bounds(real_file_space_id, file_start_coords, file_end_coords) < 0) {
		log_fatal("Failed to get start, end bounds");
		_exit(EXIT_FAILURE);
	}

	size_t mem_buf_size = num_elem_mem * H5Tget_size(dataset->type_id);

	hsize_t mem_coords[PARH5D_MAX_DIMENSIONS] = { 0 };
	parh5D_get_first_array_element(mem_ndims, mem_coords, mem_start_coords);

	hsize_t file_coords[PARH5D_MAX_DIMENSIONS] = { 0 };
	parh5D_get_first_array_element(file_ndims, file_coords, file_start_coords);

#ifdef METRICS_ENABLE
	parh5M_inc_dset_bytes_written(dataset, mem_buf_size);
#endif

	const char *mem_buf = buf[0];
	parh5T_tile_cache_t tile_cache = parh5T_init_tile_cache(dataset, PARH5D_WRITE_TILE_CACHE);

	for (hssize_t elem_num = 0; elem_num < num_elem_mem; elem_num++) {
		// for (int i = mem_ndims - 1; i >= 0; i--)
		// 	fprintf(stderr, "mem_coord[%d] = %ld ", i, mem_coords[i]);
		// fprintf(stderr, "\n");
		// for (int i = file_ndims - 1; i >= 0; i--)
		// 	fprintf(stderr, "file_coord[%d] = %ld ", i, file_coords[i]);
		// fprintf(stderr, "\n");

		char *elem_addr =
			parh5D_calc_elem_addr(mem_buf, mem_ndims, mem_shape, mem_coords, H5Tget_size(dataset->type_id));

		hsize_t file_elem_id = parh5D_get_id_from_coords(file_coords, file_shape, file_ndims);
		struct parh5T_tile file_tile = parh5D_map_id2tile(dataset, file_elem_id);
		parh5T_write_to_tile_cache(tile_cache, file_tile, elem_addr, H5Tget_size(dataset->type_id));

		parh5D_get_next_array_element(mem_ndims, mem_coords, mem_start_coords, mem_end_coords);
		parh5D_get_next_array_element(file_ndims, file_coords, file_start_coords, file_end_coords);
	}
	parh5T_tile_cache_evict(tile_cache);
	parh5T_destroy_tile_cache(tile_cache);

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

inline uint32_t parh5D_get_tile_size_in_elems(parh5D_dataset_t dataset)
{
	return dataset ? dataset->tile_size_in_elems : 0;
}

inline uint32_t parh5D_get_elems_size_in_bytes(parh5D_dataset_t dataset)
{
	return NULL == dataset ? 0 : dataset->type_id ? H5Tget_size(dataset->type_id) : 0;
}
