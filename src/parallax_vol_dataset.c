#include "parallax_vol_dataset.h"
#include "H5Ppublic.h"
#include "H5Spublic.h"
#include "H5Tpublic.h"
#include "H5public.h"
#include "djb2.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_group.h"
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
#define PARH5D_TILE_SIZE_IN_ELEMS 32

#define PARH5D_PAR_CHECK_ERROR(X)                                 \
	if (X) {                                                  \
		log_fatal("Parallax reported this error: %s", X); \
		_exit(EXIT_FAILURE);                              \
	}

struct parh5D_tile {
	uint64_t uuid;
	hid_t mem_type;
	uint32_t size_in_bytes; /*in bytes*/
	char *tile_buffer;
	bool malloced;
};

typedef enum parh5D_cursor { PARH5D_WRITE_CURSOR = 1, PARH5D_READ_CURSOR } parh5D_cursor_type_e;

struct parh5D_tile_cursor {
	hsize_t shape[PARH5D_MAX_DIMENSIONS]; /* dimensions of the storage array*/
	hsize_t start_coords[PARH5D_MAX_DIMENSIONS]; /* start in the storage array*/
	hsize_t end_coords[PARH5D_MAX_DIMENSIONS]; /* end in the storage array*/
	struct parh5D_tile tile;
	parh5D_dataset_t dataset;
	hid_t memtype;
	void *mem_buf; /*applications in memory array*/
	int nelems; /* of the in memory array*/
	int mem_elem_id; /*id of the in-memory array*/
	int ndims; /* of the storage array*/
	parh5D_cursor_type_e cursor_type; /*read or write cursor*/
};

struct parh5D_dataset {
	parh5_object_e obj_type;
	parh5G_group_t group; /*Where does this dataset belongs*/
	char *name;
	uint64_t uuid;
	hid_t space_id; /*info about the space*/
	hid_t type_id; /*info about its schema*/
	hid_t dcpl_id; /*dataset creation property list*/
};

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
		coords[dim_id] = elem_id % shape[dim_id];
		elem_id /= shape[dim_id];
		coords[dim_id] = start[dim_id] + coords[dim_id];
		if (coords[dim_id] > end[dim_id]) {
			log_debug("out of bounds");
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
static inline int parh5D_get_id_from_coords(hsize_t coords[], hsize_t shape[], int ndims)
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
 * @brief Given starting coordinates and dateset uuid calculates the tile uuid
 * used to store the tile in the KV store.
 * @param [in] dset_uuid uuid of the dataset
 * @param [in] element_id the id of the element in the projection of the array in 1D
 * @return the uuid of the tile
 */
static uint64_t parh5D_create_tile_uuid(uint64_t dset_uuid, uint64_t element_id)
{
	uint64_t coords[2] = { dset_uuid, element_id };
	return djb2_hash((const unsigned char *)coords, sizeof(coords));
}

/**
 * @brief Fetches the the tile with uuid from Parallax
 * @param [in] cursor reference to the cursor object
 * @param [in] tile_uuid the uuid of the tile to fetch
 * @param [in] tile_buffer where to store the tile
 * @param [in] tile_size the size of the buffer in bytes
 * @return true if the tile is found otherwise false
 */
static bool parh5D_fetch_tile(struct parh5D_tile_cursor *cursor, uint64_t tile_uuid, char tile_buffer[],
			      size_t tile_size)
{
	struct par_key par_key = { .size = sizeof(tile_uuid), .data = (char *)&tile_uuid };
	struct par_value par_value = { .val_buffer = tile_buffer, .val_buffer_size = tile_size };
	const char *error = NULL;
	par_get(parh5G_get_parallax_db(cursor->dataset->group), &par_key, &par_value, &error);
	assert(par_value.val_buffer == NULL || par_value.val_size == tile_size);
	if (error)
		log_debug("Tile with uuid: %lu does not exist", tile_uuid);
	return NULL == error ? true : false;
}
/**
 * @brief Fetches the the tile with uuid from parallax
 * @param [in] cursor reference to the cursor object
 * @param [in] tile_uuid the uuid of the tile to fetch
 * @param [out] partial_buffer the buffer where the partial tile is placed
 * @param [in] tile_offt (in bytes) in the tile where the copy begins
 * @param [in] partial_buffer_start offset (in bytes) in the partial buffer where the copy begins
 * @param [in] size (in bytes) of how much data to transfer in the partia_buffer
 * @return true on success otherwise false if the tile is not found
 */
static bool parh5D_fetch_partial_tile(struct parh5D_tile_cursor *cursor, uint64_t tile_uuid, char partial_buffer[],
				      int tile_offt, int partial_buffer_start, int size)
{
	struct par_key par_key = { .size = sizeof(tile_uuid), .data = (char *)&tile_uuid };
	struct par_value par_value = { 0 };
	const char *error = NULL;
	par_get(parh5G_get_parallax_db(cursor->dataset->group), &par_key, &par_value, &error);
	if (error) {
		log_debug("Tile with uuid: %lu does not exist", tile_uuid);
		return false;
	}
	memcpy(&partial_buffer[partial_buffer_start], &par_value.val_buffer[tile_offt], size);
	free(par_value.val_buffer);
	return true;
}

/**
 * @brief Fetches the the tile with uuid from parallax
 * @param [in] dataset reference to dataset object
 * @param [in] tile_uuid the uuid of the tile to fetch
 * @param [in] tile_size in bytes
 */
static void parh5D_write_tile(parh5D_dataset_t dataset, uint64_t tile_uuid, char buffer[], size_t tile_size)
{
	//Sorry misalinged access need to fetch it
	struct par_key par_key = { .size = sizeof(tile_uuid), .data = (char *)&tile_uuid };
	struct par_value par_value = { .val_buffer = buffer, .val_size = tile_size };
	const char *error = NULL;
	struct par_key_value KV = { .k = par_key, .v = par_value };
	par_put(parh5G_get_parallax_db(dataset->group), &KV, &error);
	if (error) {
		log_debug("Failed to write tile with uuid: %lu reason: %s", tile_uuid, error);
		_exit(EXIT_FAILURE);
	}
}

/**
 * @brief Creates a cursor for iterating an array in tile granularity
 * @param [in] dataset pointer to the dataset object
 * @param [in] mem pointer to the memory array
 * @param [in] memtype type of objects in memory
 * @param [in] file_space object reference to file space
 * @param [in] type of cursor (READ or WRITE)
 * @return reference to the wcursor object on SUCCESS otherwise NULL
 */
static struct parh5D_tile_cursor *parh5D_create_tile_cursor(parh5D_dataset_t dataset, void *mem, hid_t memtype,
							    hid_t file_space, parh5D_cursor_type_e cursor_type)
{
	struct parh5D_tile_cursor *cursor = calloc(1UL, sizeof(*cursor));
	cursor->cursor_type = cursor_type;
	cursor->dataset = dataset;
	cursor->mem_buf = mem;
	cursor->mem_elem_id = 0;
	cursor->memtype = memtype;
	cursor->ndims = H5Sget_simple_extent_ndims(file_space);
	if (cursor->ndims < 0) {
		log_fatal("Failed to get dimensions");
		_exit(EXIT_FAILURE);
	}
	if (H5Sget_simple_extent_dims(file_space, cursor->shape, NULL) < 0) {
		log_fatal("Failed to get the shape of the array");
		_exit(EXIT_FAILURE);
	}

	/* Get number of elements in selections */
	cursor->nelems = H5Sget_select_npoints(file_space);
	H5Sget_select_bounds(file_space, cursor->start_coords, cursor->end_coords);

	hsize_t coords[PARH5D_MAX_DIMENSIONS];
	if (!parh5D_get_coordinates(cursor->mem_elem_id, cursor->ndims, cursor->shape, cursor->start_coords,
				    cursor->end_coords, coords)) {
		log_fatal("Failed to get coordinates");
		_exit(EXIT_FAILURE);
	}

	uint32_t storage_elem_id = parh5D_get_id_from_coords(coords, cursor->shape, cursor->ndims);
	uint32_t storage_tile_id = storage_elem_id;
	uint32_t actual_tile_size = PARH5D_TILE_SIZE_IN_ELEMS;
	if (storage_elem_id % PARH5D_TILE_SIZE_IN_ELEMS) {
		storage_tile_id = storage_elem_id - (storage_elem_id % PARH5D_TILE_SIZE_IN_ELEMS);
		actual_tile_size = PARH5D_TILE_SIZE_IN_ELEMS - (storage_elem_id % PARH5D_TILE_SIZE_IN_ELEMS);
	}

	cursor->tile.uuid = parh5D_create_tile_uuid(dataset->uuid, storage_tile_id);
	cursor->tile.tile_buffer = mem;
	cursor->tile.size_in_bytes = H5Tget_size(cursor->memtype) * PARH5D_TILE_SIZE_IN_ELEMS;
	cursor->mem_elem_id = PARH5D_TILE_SIZE_IN_ELEMS;

	if (PARH5D_READ_CURSOR == cursor->cursor_type && storage_tile_id == storage_elem_id) {
		if (!parh5D_fetch_tile(cursor, cursor->tile.uuid, mem, cursor->tile.size_in_bytes)) {
			log_fatal("Tile with uuid %lu does not exist! (It should)", cursor->tile.uuid);
			_exit(EXIT_FAILURE);
		}
		return cursor;
	}

	if (PARH5D_READ_CURSOR == cursor->cursor_type && storage_tile_id != storage_elem_id) {
		int offt_in_tile = H5Tget_size(cursor->memtype) * (storage_elem_id % PARH5D_TILE_SIZE_IN_ELEMS);
		int size = H5Tget_size(cursor->memtype) *
			   (PARH5D_TILE_SIZE_IN_ELEMS - storage_tile_id % PARH5D_TILE_SIZE_IN_ELEMS);
		if (!parh5D_fetch_partial_tile(cursor, cursor->tile.uuid, cursor->mem_buf, offt_in_tile, 0, size)) {
			log_fatal("Tile with uuid %lu does not exist! (It should)", cursor->tile.uuid);
			_exit(EXIT_FAILURE);
		}
		cursor->tile.size_in_bytes = size;
		return cursor;
	}

	/*The first tile is the one of the in memory array*/
	if (PARH5D_WRITE_CURSOR == cursor->cursor_type && storage_tile_id == storage_elem_id)
		return cursor;

	//Sorry misalinged or read access need to fetch it
	cursor->tile.tile_buffer = calloc(1UL, cursor->tile.size_in_bytes);
	cursor->tile.malloced = true;
	parh5D_fetch_tile(cursor, cursor->tile.uuid, cursor->tile.tile_buffer, cursor->tile.size_in_bytes);

	memcpy(&cursor->tile.tile_buffer[storage_elem_id % PARH5D_TILE_SIZE_IN_ELEMS], cursor->mem_buf,
	       actual_tile_size * H5Tget_size(memtype));
	cursor->mem_elem_id = actual_tile_size;

	return cursor;
}

/**
 * @brief Frees (if needed) the tile buffer
 * @param [in] tile pointer to the tile object
 */
static void parh5D_free_tile_buffer(struct parh5D_tile *tile)
{
	if (!tile->malloced)
		return;
	free(tile->tile_buffer);
	tile->tile_buffer = NULL;
	tile->malloced = false;
}

/**
 * @brief Advances the cursor and fetches the next tile of the array.
 * @param [in] wcursor pointer to the cursor object
 * @return true on success false if end of the array has reached
 */
static bool parh5D_advance_tile_wcursor(struct parh5D_tile_cursor *cursor)
{
	if (cursor->mem_elem_id >= cursor->nelems) {
		log_debug("End of cursor");
		return false;
	}

	hsize_t coords[PARH5D_MAX_DIMENSIONS];
	if (!parh5D_get_coordinates(cursor->mem_elem_id, cursor->ndims, cursor->shape, cursor->start_coords,
				    cursor->end_coords, coords)) {
		log_fatal("Failed to get coordinates");
		_exit(EXIT_FAILURE);
	}
	uint32_t storage_elem_id = parh5D_get_id_from_coords(coords, cursor->shape, cursor->ndims);
	cursor->tile.uuid = parh5D_create_tile_uuid(cursor->dataset->uuid, storage_elem_id);

	parh5D_free_tile_buffer(&cursor->tile);

	cursor->tile.tile_buffer = (char *)cursor->mem_buf + cursor->mem_elem_id * H5Tget_size(cursor->memtype);
	uint32_t remaining_in_mem_buffer = cursor->nelems - cursor->mem_elem_id;

	if (remaining_in_mem_buffer >= PARH5D_TILE_SIZE_IN_ELEMS) {
		cursor->mem_elem_id += PARH5D_TILE_SIZE_IN_ELEMS;
		return PARH5D_WRITE_CURSOR == cursor->cursor_type ?
			       true :
			       parh5D_fetch_tile(cursor, cursor->tile.uuid, cursor->tile.tile_buffer,
						 cursor->tile.size_in_bytes);
	}
	//misalinged read access
	if (PARH5D_READ_CURSOR == cursor->cursor_type) {
		cursor->mem_elem_id += remaining_in_mem_buffer;
		cursor->tile.size_in_bytes = remaining_in_mem_buffer * H5Tget_size(cursor->memtype);
		return parh5D_fetch_partial_tile(cursor, cursor->tile.uuid, cursor->tile.tile_buffer, 0, 0,
						 cursor->tile.size_in_bytes);
	}

	cursor->tile.tile_buffer = calloc(1UL, cursor->tile.size_in_bytes);
	cursor->tile.malloced = true;
	parh5D_fetch_tile(cursor, cursor->tile.uuid, cursor->tile.tile_buffer, cursor->tile.size_in_bytes);
	memcpy(cursor->tile.tile_buffer, (char *)cursor->mem_buf + cursor->mem_elem_id * H5Tget_size(cursor->memtype),
	       remaining_in_mem_buffer * H5Tget_size(cursor->memtype));
	return true;
}

static void parh5D_close_cursor(struct parh5D_tile_cursor *cursor)
{
	parh5D_free_tile_buffer(&cursor->tile);
	free(cursor);
}
/**
 * @brief Given a dataset name creates a unique uuid
 * @param [in] dataset pointer to the dataset object
 * @param [in] name C string contains name of the dataset
 * @return the uuid of the dataset
 */
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

#define PAR5HD_BUFFER_CHECK_REMAINING(X, Y) \
	if (X < Y) {                        \
		free(buffer);               \
		buffer_size *= 2;           \
		continue;                   \
	}

#define PAR5HD_BUFFER_SIZE 256
static char *parh5D_serialize_dataset(struct parh5D_dataset *dset, size_t *buf_size)
{
	size_t buffer_size = PAR5HD_BUFFER_SIZE;
	size_t remaining_bytes = PAR5HD_BUFFER_SIZE;
	size_t idx = 0;
	char *buffer = NULL;
	while (1) {
		idx = 0;
		buffer = calloc(1UL, buffer_size);
		remaining_bytes = buffer_size;
		size_t name_size = strlen(dset->name) + 1;
		size_t space_needed = sizeof(name_size) + name_size;
		PAR5HD_BUFFER_CHECK_REMAINING(remaining_bytes, space_needed);
		//Dataset's name string size
		memcpy(&buffer[idx], &name_size, sizeof(name_size));
		idx += sizeof(name_size);
		//Dataset's actual name
		memcpy(&buffer[idx], dset->name, name_size);
		idx += name_size;
		remaining_bytes -= space_needed;
		//Dataset's uuid
		space_needed = sizeof(dset->uuid);
		PAR5HD_BUFFER_CHECK_REMAINING(remaining_bytes, space_needed);
		memcpy(&buffer[idx], &dset->uuid, space_needed);
		idx += space_needed;
		remaining_bytes -= space_needed;
		//Dataset's space_id
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
		*buf_size = buffer_size - remaining_bytes;
		return buffer;
	}
}

/**
 * @brief Deserializes the contents of a buffer into a dataspace object
 * @param [in] dataset pointer to the daset object which contents we fill
 * @param [in] buffer pointer to the buffer which contains the serialized data
 * @param [in] buffer_size the size of the buffer
 */
static void parh5D_deserialize_dataset(parh5D_dataset_t dataset, char *buffer, size_t buffer_size)
{
	(void)buffer_size;
	size_t idx = 0;
	size_t name_size = 0;
	//get the name
	memcpy(&name_size, &buffer[idx], sizeof(name_size));
	idx += sizeof(name_size);
	dataset->name = calloc(1UL, name_size);
	memcpy(dataset->name, &buffer[idx], name_size);
	idx += name_size;
	//get the dataset uuid
	memcpy(&dataset->uuid, &buffer[idx], sizeof(dataset->uuid));
	idx += sizeof(dataset->uuid);
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

static bool parh5G_store_dataset(parh5D_dataset_t dataset, const char **error_message)
{
	uint64_t group_uuid = parh5G_get_group_uuid(dataset->group);
	if (!group_uuid) {
		log_fatal("Group uuid not set!");
		assert(0);
		_exit(EXIT_FAILURE);
	}
	struct par_key group_key = { .size = sizeof(group_uuid), .data = (char *)&group_uuid };
	struct par_value par_value = { 0 };
	const char *error = NULL;
	par_get(parh5G_get_parallax_db(dataset->group), &group_key, &par_value, &error);
	if (error) {
		log_debug("Could not find key: %.*s reason: %s", group_key.size, group_key.data, error);
		return false;
	}
	/*Unroll dataset*/
	char *value = par_value.val_buffer;
	char *result = strstr(par_value.val_buffer, dataset->name);

	if (result != NULL) {
		log_debug("Dataset already exists %s\n", dataset->name);
		*error_message = "Dataset exists";
		return false;
	}
	size_t buffer_size = 0;
	char *buffer = parh5D_serialize_dataset(dataset, &buffer_size);
	struct par_key dataset_key = { .size = sizeof(dataset->uuid), .data = (char *)&dataset->uuid };
	struct par_value new_par_value = { .val_buffer_size = buffer_size,
					   .val_size = buffer_size,
					   .val_buffer = buffer };
	struct par_key_value KV = { .k = dataset_key, .v = new_par_value };
	par_put(parh5G_get_parallax_db(dataset->group), &KV, &error);

	if (error) {
		log_fatal("Failed to add dataset %s", dataset->name);
		_exit(EXIT_FAILURE);
	}
	free(value);
	return true;
}

void *parh5D_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id,
		    hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
	(void)type_id;
	(void)loc_params;
	(void)name;
	(void)lcpl_id;
	(void)dcpl_id;
	(void)dapl_id;
	(void)req;
	(void)dxpl_id;
	parh5_object_e *obj_type = (parh5_object_e *)obj;

	if (PAR_H5_GROUP != *obj_type) {
		log_fatal("Dataset can only be associated with a group object");
		_exit(EXIT_FAILURE);
	}
	parh5G_group_t group = obj;

	parh5D_dataset_t dataset = calloc(1UL, sizeof(struct parh5D_dataset));
	dataset->obj_type = PAR_H5_DATASET;
	dataset->type_id = H5Tcopy(type_id);
	dataset->dcpl_id = H5Pcopy(dcpl_id);
	log_debug("Creating dataspace for group uuid: %lu", parh5G_get_group_uuid(group));
	dataset->group = group;
	dataset->space_id = H5Scopy(space_id);
	dataset->uuid = parh5D_create_uuid(dataset, name);
	log_debug("Dimensions of new dataspace are %d", H5Sget_simple_extent_ndims(dataset->space_id));
	const char *error_message = NULL;
	parh5G_store_dataset(dataset, &error_message);

	return dataset;
}

void *parh5D_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id,
		  void **req)
{
	(void)loc_params;
	(void)dapl_id;
	(void)dxpl_id;
	(void)req;
	parh5_object_e *obj_type = (parh5_object_e *)obj;

	if (PAR_H5_GROUP != *obj_type) {
		log_fatal("Dataset can only be associated with a group object");
		_exit(EXIT_FAILURE);
	}
	parh5G_group_t group = obj;
	parh5D_dataset_t dataset = calloc(1UL, sizeof(struct parh5D_dataset));
	dataset->obj_type = PAR_H5_DATASET;
	dataset->group = group;
	dataset->uuid = parh5D_create_uuid(dataset, name);
	struct par_key par_key = { .size = sizeof(dataset->uuid), .data = (char *)&dataset->uuid };
	struct par_value par_value = { 0 };
	const char *error = NULL;
	par_get(parh5G_get_parallax_db(dataset->group), &par_key, &par_value, &error);
	if (error) {
		log_debug("Dataset with uuid: %lu does not exist reason: %s", dataset->uuid, error);
		free(dataset);
		return NULL;
	}

	log_debug("Found dataset with name: %s and uuid: %lu of size: %u proceeding to deserialize it...", name,
		  dataset->uuid, par_value.val_size);
	parh5D_deserialize_dataset(dataset, par_value.val_buffer, par_value.val_size);
	free(par_value.val_buffer);
	log_debug("Deserialization success of %s", name);
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

	parh5_object_e *obj_type = (parh5_object_e *)dset[0];
	if (PAR_H5_DATASET != *obj_type) {
		log_fatal("Dataset write can only be associated with a file object");
		_exit(EXIT_FAILURE);
	}

	parh5D_dataset_t dataset = (parh5D_dataset_t)dset[0];

	/* Get dataspace extent */
	int dpace_ndims = 0;
	if ((dpace_ndims = H5Sget_simple_extent_ndims(dataset->space_id)) < 0) {
		log_fatal("can't get number of dimensions");
		_exit(EXIT_FAILURE);
	}

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

	struct parh5D_tile_cursor *cursor =
		parh5D_create_tile_cursor(dataset, buf[0], mem_type_id[0], real_file_space_id, PARH5D_READ_CURSOR);
	for (bool ret = true; cursor != NULL && ret; ret = parh5D_advance_tile_wcursor(cursor))
		;
	parh5D_close_cursor(cursor);
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

	parh5_object_e *obj_type = (parh5_object_e *)dset[0];
	if (PAR_H5_DATASET != *obj_type) {
		log_fatal("Dataset write can only be associated with a file object");
		_exit(EXIT_FAILURE);
	}

	parh5D_dataset_t dataset = (parh5D_dataset_t)dset[0];

	/* Get dataspace extent */
	int dpace_ndims = 0;
	if ((dpace_ndims = H5Sget_simple_extent_ndims(dataset->space_id)) < 0) {
		log_fatal("can't get number of dimensions");
		_exit(EXIT_FAILURE);
	}

	hid_t real_file_space_id = file_space_id[0] == H5S_ALL ? dataset->space_id : file_space_id[0];
	hid_t real_mem_space_id = mem_space_id[0] == H5S_ALL ? dataset->space_id : mem_space_id[0];

	hssize_t num_elem_mem = -1;
	if ((num_elem_mem = H5Sget_select_npoints(real_mem_space_id)) < 0) {
		log_fatal("can't get number of points in memory selection");
		assert(0);
		_exit(EXIT_FAILURE);
	}
	log_debug("Mem n points are: %ld", num_elem_mem);

	/* Get number of elements in selections */
	hssize_t num_elem_file = -1;
	if ((num_elem_file = H5Sget_select_npoints(real_file_space_id)) < 0) {
		log_fatal("can't get number of points in file selection for dataset: %s", dataset->name);
		_exit(EXIT_FAILURE);
	}

	/* Various sanity and special case checks */
	if (num_elem_file != num_elem_mem)
		log_fatal("number of elements selected in file and memory dataspaces is different");

	if (num_elem_file && !buf)
		log_fatal("write buffer is NULL but selection has >0 elements");

	if (num_elem_file == 0)
		return PARH5_SUCCESS;

	hsize_t mem_shape[PARH5D_MAX_DIMENSIONS] = { 0 };
	int mem_dimensions = H5Sget_simple_extent_dims(real_mem_space_id, mem_shape, NULL);
	log_debug("Mem space dimensions: %d", mem_dimensions);
	for (int i = 0; i < mem_dimensions; i++)
		log_debug("mem dimension[%d] = %ld", i, mem_shape[i]);
	hsize_t file_shape[PARH5D_MAX_DIMENSIONS] = { 0 };
	int file_dimensions = H5Sget_simple_extent_dims(real_file_space_id, file_shape, NULL);
	log_debug("File space dimensions %d", file_dimensions);
	for (int i = 0; i < file_dimensions; i++)
		log_debug("file dimension[%d] = %ld", i, file_shape[i]);
	hsize_t start_coords[PARH5D_MAX_DIMENSIONS];
	hsize_t end_coords[PARH5D_MAX_DIMENSIONS];
	H5Sget_select_bounds(real_file_space_id, start_coords, end_coords);
	for (int i = 0; i < file_dimensions; i++) {
		log_debug("start dimension[%d] = %ld", i, start_coords[i]);
	}
	for (int i = 0; i < file_dimensions; i++) {
		log_debug("end dimension[%d] = %ld", i, end_coords[i]);
	}

	struct parh5D_tile_cursor *cursor = parh5D_create_tile_cursor(dataset, (void *)buf[0], mem_type_id[0],
								      real_file_space_id, PARH5D_WRITE_CURSOR);
	for (bool ret = true; cursor != NULL && ret; ret = parh5D_advance_tile_wcursor(cursor)) {
		log_debug("Writing tile uuid: %lu....", cursor->tile.uuid);
		parh5D_write_tile(cursor->dataset, cursor->tile.uuid, cursor->tile.tile_buffer,
				  cursor->tile.size_in_bytes);
		// log_debug("Writing tile uuid: %lu SUCCESS", cursor->tile.uuid);
	}
	parh5D_close_cursor(cursor);
	return PARH5_SUCCESS;
}

herr_t parh5D_get(void *obj, H5VL_dataset_get_args_t *get_op, hid_t dxpl_id, void **req)
{
	(void)dxpl_id;
	(void)req;
	parh5_object_e *obj_type = (parh5_object_e *)obj;
	if (PAR_H5_DATASET != *obj_type) {
		log_fatal("Object is not a dataset!");
		_exit(EXIT_FAILURE);
	}
	parh5D_dataset_t dataset = obj;

	switch (get_op->op_type) {
	case H5VL_DATASET_GET_SPACE:
		log_debug("HDF5 wants to know about space of dataset: %s", dataset->name);
		get_op->args.get_space.space_id = dataset->space_id;
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
	(void)dset;
	(void)dxpl_id;
	(void)req;
	parh5_object_e *obj_type = (parh5_object_e *)dset;
	if (PAR_H5_DATASET != *obj_type) {
		log_fatal("Dataset write can only be associated with a file object");
		_exit(EXIT_FAILURE);
	}
	parh5D_dataset_t dataset = dset;
	H5Sclose(dataset->space_id);
	H5Pclose(dataset->dcpl_id);
	H5Tclose(dataset->type_id);
	free(dataset->name);
	free(dataset);
	return PARH5_SUCCESS;
}
