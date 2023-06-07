#include "parallax_vol_group.h"
#include "djb2.h"
#include "parallax/structures.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_file.h"
#include "parallax_vol_inode.h"
#include <assert.h>
#include <log.h>
#include <parallax/parallax.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct parh5G_group {
	parh5F_file_t file;
	struct parh5I_inode *inode;
};

// /**
//  * @brief Saves group info in parallax
//  * @param group reference to the group object
//  * @return true on success false on failure
//  */
// static bool parh5G_save_group(parh5G_group_t group)
// {
// 	if (!parh5G_check_group_params(group)) {
// 		log_debug("File and/or group name have not been set");
// 		_exit(EXIT_FAILURE);
// 	}

// #define PARH5G_VALUE_BUFFER_SIZE 128
// 	assert(group);
// 	char value_buffer[PARH5G_VALUE_BUFFER_SIZE] = { 0 };
// 	char *value = value_buffer;
// 	/*we keep in parallax in the value field group_name and file name*/
// 	if (strlen(group->group_name) + strlen(parh5F_get_file_name(group->file)) + 2 > PARH5G_VALUE_BUFFER_SIZE)
// 		value = calloc(1UL, strlen(group->group_name) + strlen(parh5F_get_file_name(group->file)) + 2UL);

// 	int idx = strlen(group->group_name);
// 	memcpy(&value[0], group->group_name, idx);
// 	memcpy(&value[idx], ":", strlen(":"));
// 	idx += strlen(":");
// 	memcpy(&value[idx], parh5F_get_file_name(group->file), strlen(parh5F_get_file_name(group->file)));
// 	idx += strlen(parh5F_get_file_name(group->file));
// 	idx++;
// 	struct par_key par_key = { .size = sizeof(group->inode_num), .data = (char *)&group->inode_num };
// 	struct par_value par_value = { .val_size = idx, .val_buffer = value };
// 	const char *error_message = NULL;
// 	struct par_key_value KV = { .k = par_key, .v = par_value };
// 	par_put(parh5G_get_parallax_db(group), &KV, &error_message);

// 	if (value != value_buffer)
// 		free(value);

// 	if (error_message) {
// 		log_fatal("Failed to insert group: %s reason: %s", group->group_name, error_message);
// 		_exit(EXIT_FAILURE);
// 	}
// 	return true;
// }

// bool parh5G_add_dataset(parh5G_group_t group, const char *dataset_name, const char **error_message)
// {
// 	if (!group->uuid_valid) {
// 		log_fatal("Group uuid not set!");
// 		_exit(EXIT_FAILURE);
// 	}
// 	struct par_key par_key = { .size = sizeof(group->inode_num), .data = (char *)&group->inode_num };
// 	struct par_value par_value = { 0 };
// 	const char *error = NULL;
// 	par_get(parh5G_get_parallax_db(group), &par_key, &par_value, &error);
// 	if (error) {
// 		log_debug("Could not find key: %.*s reason: %s", par_key.size, par_key.data, error);
// 		return false;
// 	}
// 	/*Unroll dataset*/
// 	char *value = par_value.val_buffer;
// 	log_debug("Searching dataset %s into group's datasets: %s", dataset_name, par_value.val_buffer);
// 	char *result = strstr(par_value.val_buffer, dataset_name);

// 	if (result != NULL) {
// 		log_debug("Dataset already exists %s\n", dataset_name);
// 		*error_message = "Dataset exists";
// 		return false;
// 	}
// 	/*XXX TODO XXX add upserts in parallax*/
// 	char *new_value = calloc(1UL, strlen(par_value.val_buffer) + 1UL /*space*/ + strlen(dataset_name) + 1UL /*\0*/);
// 	memcpy(new_value, value, strlen(value));
// 	int idx = strlen(value);
// 	memcpy(&new_value[idx], ":", 2UL);
// 	idx++;
// 	memcpy(&new_value[idx], dataset_name, strlen(dataset_name));
// 	idx += strlen(dataset_name) + 2UL;
// 	struct par_value new_par_value = { .val_size = idx, .val_buffer = new_value };
// 	struct par_key_value KV = { .k = par_key, .v = new_par_value };
// 	par_put(parh5G_get_parallax_db(group), &KV, &error);

// 	if (error) {
// 		log_fatal("Failed to add dataset %s in group %s", dataset_name, group->group_name);
// 		_exit(EXIT_FAILURE);
// 	}
// 	free(value);
// 	return true;
// }

/**
  * @brief Maps a group to a parallax DB
  * @param group reference to the group object
  * @return true on success false on error
*/
// static bool parh5G_map_group_to_db(parh5G_group_t group)
// {
// 	assert(group);
// 	if (!group)
// 		return false;
// 	group->par_db = parh5F_get_parallax_db(group->file);
// 	return true;
// }

parh5G_group_t parh5G_new_group(parh5F_file_t file, const char *name)
{
	parh5G_group_t group = calloc(1UL, sizeof(struct parh5G_group));
	group->file = file;
	parh5I_inode_t inode = parh5I_create_inode(name, PARH5_GROUP, NULL, parh5F_get_parallax_db(file));
	parh5I_store_inode(inode, parh5F_get_parallax_db(file));
	group->inode = inode;

	return group;
}

const char *parh5G_get_group_name(parh5G_group_t group)
{
	return group ? parh5I_get_inode_name(group->inode) : NULL;
}

/**
 * @brief Creates a new group object
*/
void *parh5G_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id,
		    hid_t gapl_id, hid_t dxpl_id, void **req)
{
	(void)loc_params;
	(void)lcpl_id;
	(void)gcpl_id;
	(void)gapl_id;
	(void)dxpl_id;
	(void)req;

	parh5_object_e *obj_type = (parh5_object_e *)obj;
	parh5G_group_t parent_group = NULL;

	if (PARH5_FILE == *obj_type) {
		parh5F_file_t file = (parh5F_file_t)obj;
		parent_group = parh5F_get_root_group(file);
	} else if (PARH5_GROUP == *obj_type)
		parent_group = (parh5G_group_t)obj;
	else {
		log_fatal("Parent can be either a group or file");
		_exit(EXIT_FAILURE);
	}

	/*first do a search in "parent to see if group is already there"*/
	if (parh5I_bsearch_inode(parent_group->inode, name) == 0) {
		log_fatal("group %s already exists", name);
		_exit(EXIT_FAILURE);
	}
	parh5G_group_t new_group = parh5G_new_group(parent_group->file, name);
	//save inode to parallax
	parh5I_store_inode(new_group->inode, parh5F_get_parallax_db(parent_group->file));
	//inform the parent
	if (!parh5I_add_inode(parent_group->inode, parh5I_get_inode_num(new_group->inode), name)) {
		log_fatal("inode of parent group need resizing XXX TODO XXX");
		_exit(EXIT_FAILURE);
	};
	return new_group;
}

void *parh5G_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id,
		  void **req)
{
	(void)gapl_id;
	(void)loc_params;
	(void)name;
	(void)dxpl_id;
	(void)req;
	// log_debug("<group_properties>");
	// H5Piterate(gapl_id, NULL, parh5_property_list_iterator, NULL);
	// log_debug("</group_properties>");
	parh5_object_e *obj_type = (parh5_object_e *)obj;
	parh5G_group_t parent_group = NULL;

	if (PARH5_FILE == *obj_type) {
		parh5F_file_t file = (parh5F_file_t)obj;
		parent_group = parh5F_get_root_group(file);
	} else if (PARH5_GROUP == *obj_type)
		parent_group = (parh5G_group_t)obj;
	else {
		log_fatal("Parent can be either a group or file");
		_exit(EXIT_FAILURE);
	}
	uint64_t inode_num = parh5I_bsearch_inode(parent_group->inode, name);

	if (inode_num) {
		parh5I_inode_t inode = parh5I_get_inode(parh5F_get_parallax_db(parent_group->file), inode_num);

		if (!inode) {
			log_fatal("Could not find inode_num: %lu I should have!", inode_num);
			_exit(EXIT_FAILURE);
		}
		parh5G_group_t new_group = calloc(1UL, sizeof(*new_group));
		new_group->file = parent_group->file;
		new_group->inode = inode;
		return new_group;
	}
	log_debug("group: %s does not exist, creating a new one", name);

	parh5G_group_t new_group = parh5G_new_group(parent_group->file, name);
	//save inode to parallax
	parh5I_store_inode(new_group->inode, parh5F_get_parallax_db(parent_group->file));
	//inform the parent
	if (!parh5I_add_inode(parent_group->inode, parh5I_get_inode_num(new_group->inode), name)) {
		log_fatal("inode of parent group needs resizing XXX TODO XXX");
		_exit(EXIT_FAILURE);
	};
	return new_group;
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
	log_fatal("Group: Sorry unimplemented function XXX TODO XXX");
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
	if (PARH5_GROUP != *obj_type) {
		log_fatal("Object is not a Group object!");
		_exit(EXIT_FAILURE);
	}
	free(grp);
	return PARH5_SUCCESS;
}
