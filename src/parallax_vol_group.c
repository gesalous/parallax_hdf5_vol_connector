#include "parallax_vol_group.h"
#include "djb2.h"
#include "parallax/structures.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_file.h"
#include <assert.h>
#include <log.h>
#include <parallax/parallax.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct parh5G_group {
	parh5_object_e obj_type;
	uint64_t uuid;
	uint64_t par_group_id;
	parh5F_file_t file;
	par_handle par_db;
	const char *group_name;
	bool uuid_valid;
};

/**
 * @brief Checks if the file and the name of the group has
 * been set
 * @param reference to the group object
 * @return true if it is healthy false on error
 */
static inline bool parh5G_check_group_params(parh5G_group_t group)
{
	assert(group);
	return !group->group_name || !group->file ? false : true;
}

/**
  * @brief Returns the parallax db associated with this group
  * @param group reference to the group object
  * @return parallax db reference or NULL on error
*/
inline par_handle parh5G_get_parallax_db(parh5G_group_t group)
{
	assert(group);
	return group ? group->par_db : NULL;
}

/**
 * @brief Constructs a group key (decorator pattern)
 * @param group_name group name reference
 * @return true on success false on failure
 */
static bool parh5G_construct_group_uuid(parh5G_group_t group)
{
	assert(group);
	if (!parh5G_check_group_params(group)) {
		log_debug("File and/or group name have not been set");
		_exit(EXIT_FAILURE);
	}
	group->uuid = djb2_hash((unsigned char *)group->group_name, strlen(group->group_name));
	log_debug("group uuid = %lu", group->uuid);
	group->uuid_valid = true;
	return true;
}

/**
 * @brief Checks if a group exists in parallax
 * @param group pointer to the group object
 * @return true if it exists either false.
 */
static bool parh5G_group_exists(parh5G_group_t group)
{
	assert(group);
	if (!parh5G_check_group_params(group)) {
		log_debug("File and/or group name have not been set");
		_exit(EXIT_FAILURE);
	}

	if (!group->uuid_valid) {
		log_warn("uuid has not been set");
		_exit(EXIT_FAILURE);
	}
	/* do a lookup in parallax*/
	struct par_key par_key = { .size = sizeof(group->uuid), .data = (const char *)&group->uuid };
	log_debug("Checking if group: %lu exists", group->uuid);
	return par_exists(parh5G_get_parallax_db(group), &par_key) == PAR_SUCCESS ? true : false;
}

/**
 * @brief Saves group info in parallax
 * @param group reference to the group object
 * @return true on success false on failure
 */
static bool parh5G_save_group(parh5G_group_t group)
{
	if (!parh5G_check_group_params(group)) {
		log_debug("File and/or group name have not been set");
		_exit(EXIT_FAILURE);
	}

#define PARH5G_VALUE_BUFFER_SIZE 128
	assert(group);
	char value_buffer[PARH5G_VALUE_BUFFER_SIZE] = { 0 };
	char *value = value_buffer;
	/*we keep in parallax in the value field group_name and file name*/
	if (strlen(group->group_name) + strlen(parh5F_get_file_name(group->file)) + 2 > PARH5G_VALUE_BUFFER_SIZE)
		value = calloc(1UL, strlen(group->group_name) + strlen(parh5F_get_file_name(group->file)) + 1);

	int idx = strlen(group->group_name);
	memcpy(&value[0], group->group_name, idx);
	memcpy(&value[idx], " ", strlen(" "));
	idx += strlen(" ");
	memcpy(&value[idx], parh5F_get_file_name(group->file), strlen(parh5F_get_file_name(group->file)));
	idx += strlen(parh5F_get_file_name(group->file));
	struct par_key par_key = { .size = sizeof(group->uuid), .data = (char *)&group->uuid };
	struct par_value par_value = { .val_size = idx, .val_buffer = value };
	const char *error_message = NULL;
	struct par_key_value KV = { .k = par_key, .v = par_value };
	par_put(parh5G_get_parallax_db(group), &KV, &error_message);

	if (value != value_buffer)
		free(value);

	if (error_message) {
		log_fatal("Failed to insert group: %s reason: %s", group->group_name, error_message);
		_exit(EXIT_FAILURE);
	}
	return true;
}

inline const char *parh5G_get_group_name(parh5G_group_t group)
{
	return group ? group->group_name : NULL;
}
uint64_t parh5G_get_group_uuid(parh5G_group_t group)
{
	return group ? group->uuid : 0;
}

bool parh5G_add_dataset(parh5G_group_t group, const char *dataset_name, const char **error_message)
{
	if (!group->uuid_valid) {
		log_fatal("Group uuid not set!");
		_exit(EXIT_FAILURE);
	}
	struct par_key par_key = { .size = sizeof(group->uuid), .data = (char *)&group->uuid };
	struct par_value par_value = { 0 };
	const char *error = NULL;
	par_get(parh5G_get_parallax_db(group), &par_key, &par_value, &error);
	if (error) {
		log_debug("Could not find key: %.*s reason: %s", par_key.size, par_key.data, error);
		return false;
	}
	/*Unroll dataset*/
	char *value = par_value.val_buffer;
	char *result = strstr(par_value.val_buffer, dataset_name);

	if (result != NULL) {
		log_debug("Dataset already exists %s\n", dataset_name);
		*error_message = "Dataset exists";
		return false;
	}
	/*XXX TODO XXX add upserts in parallax*/
	char *new_value = calloc(1UL, strlen(par_value.val_buffer) + 1UL /*space*/ + strlen(dataset_name) + 1UL /*\0*/);
	memcpy(new_value, value, strlen(value));
	int idx = strlen(value);
	memcpy(&new_value[idx], " ", 1UL);
	idx++;
	memcpy(&new_value[idx], dataset_name, strlen(dataset_name));
	idx += strlen(dataset_name) + 1UL;
	struct par_value new_par_value = { .val_size = idx, .val_buffer = new_value };
	struct par_key_value KV = { .k = par_key, .v = new_par_value };
	par_put(parh5G_get_parallax_db(group), &KV, &error);

	if (error) {
		log_fatal("Failed to add dataset %s in group %s", dataset_name, group->group_name);
		_exit(EXIT_FAILURE);
	}
	free(value);
	return true;
}

/**
* @brief Maps a group to a parallax DB
  * @param group reference to the group object
  * @return true on success false on error
*/
static bool parh5G_map_group_to_db(parh5G_group_t group)
{
	assert(group);
	if (!group)
		return false;
	group->par_db = parh5F_get_parallax_db(group->file);
	return true;
}

static parh5G_group_t parh5G_new_group(parh5F_file_t file, const char *name)
{
	//check if the group exists
	parh5G_group_t group = calloc(1UL, sizeof(struct parh5G_group));
	group->obj_type = PAR_H5_GROUP;
	group->file = file;
	group->group_name = strdup(name);
	parh5G_map_group_to_db(group);
	parh5G_construct_group_uuid(group);
	return group;
}

void *parh5G_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id,
		    hid_t gapl_id, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)loc_params;
	(void)name;
	(void)lcpl_id;
	(void)gcpl_id;
	(void)gapl_id;
	(void)dxpl_id;
	(void)req;

	parh5_object_e *obj_type = (parh5_object_e *)obj;
	if (PAR_H5_FILE != *obj_type) {
		log_fatal("Group can only be associated with a file object");
		_exit(EXIT_FAILURE);
	}
	parh5F_file_t file = (parh5F_file_t)obj;
	log_debug("Creating group %s for file %s", name, parh5F_get_file_name(file));
	parh5G_group_t group = parh5G_new_group(file, name);
	if (parh5G_group_exists(group)) {
		log_warn("Group: %s already exists", name);
		return NULL;
	}
	parh5G_save_group(group);
	log_debug("Created group %s SUCCESS", name);

	return group;
}

void *parh5G_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id,
		  void **req)
{
	(void)obj;
	(void)loc_params;
	(void)name;
	(void)gapl_id;
	(void)dxpl_id;
	(void)req;
	log_fatal("Group: Sorry unimplemented function XXX TODO XXX");
	_exit(EXIT_FAILURE);
	return NULL;
}

herr_t parh5G_get(void *obj, H5VL_group_get_args_t *args, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_fatal("Group: Sorry unimplemented function XXX TODO XXX");
	_exit(EXIT_FAILURE);
	return 1;
}

herr_t parh5G_specific(void *obj, H5VL_group_specific_args_t *args, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)args;
	(void)dxpl_id;
	(void)req;
	fprintf(stderr, "PAR_GROUP_class from function %s in file: %s\n", __func__, __FILE__);
	_exit(EXIT_FAILURE);
	return 1;
}

herr_t parh5G_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_fatal("Group: Sorry unimplemented function XXX TODO XXX");
	_exit(EXIT_FAILURE);
	return 1;
}

herr_t parh5G_close(void *grp, hid_t dxpl_id, void **req)
{
	(void)dxpl_id;
	(void)req;
	parh5_object_e *obj_type = (parh5_object_e *)grp;
	if (PAR_H5_GROUP != *obj_type) {
		log_fatal("Object is not a Group object!");
		_exit(EXIT_FAILURE);
	}
	free(grp);
	return PARH5_SUCCESS;
}
