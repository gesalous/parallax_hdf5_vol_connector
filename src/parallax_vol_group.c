#include "parallax_vol_group.h"
#include "murmurhash.h"
#include "parallax/structures.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_file.h"
#include "parallax_vol_inode.h"
#include <H5Ipublic.h>
#include <H5Ppublic.h>
#include <H5Spublic.h>
#include <H5VLconnector.h>
#include <assert.h>
#include <log.h>
#include <parallax/parallax.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define PARH5G_CHECK_REMAINING(X, Y)                               \
	if (X < Y) {                                               \
		log_fatal("Inode metadata buffer size too small"); \
		_exit(EXIT_FAILURE);                               \
	}
#define PARH5G_CHECK_ERROR(X)                  \
	if (X < 0) {                           \
		log_fatal("Operation failed"); \
		raise(SIGINT);                 \
		_exit(EXIT_FAILURE);           \
	}

struct parh5G_group {
	H5I_type_t type;
	parh5F_file_t file;
	struct parh5I_inode *inode;
	hid_t cpl_id;
	hid_t apl_id;
};

inline hid_t parh5G_get_cpl(parh5G_group_t group)
{
	return group ? group->cpl_id : -1;
}

inline hid_t parh5G_get_apl(parh5G_group_t group)
{
	return group ? group->apl_id : -1;
}

static void parh5G_serialize_group_metadata(parh5G_group_t group)
{
	size_t remaining = parh5I_get_inode_metadata_size();
	size_t idx = 0;
	char *metadata_buf = parh5I_get_inode_metadata_buf(group->inode);
	hid_t file_pls[2] = { group->cpl_id, group->apl_id };

	for (size_t i = 0; i < sizeof(file_pls) / sizeof(hid_t); i++) {
		size_t encoded_size = 0;
		herr_t error = H5Pencode2(file_pls[i], NULL, &encoded_size, 0);
		PARH5G_CHECK_ERROR(error);
		log_debug("Remaining %lu encoded_size needed: %lu", remaining, encoded_size);
		PARH5G_CHECK_REMAINING(remaining, encoded_size);
		error = H5Pencode2(file_pls[i], &metadata_buf[idx], &encoded_size, 0);
		PARH5G_CHECK_ERROR(error);
		PARH5G_CHECK_REMAINING(remaining, encoded_size);
		remaining -= encoded_size;
		idx += encoded_size;
	}
}

static void parh5G_deserialize_group_metadata(parh5G_group_t group)
{
	char *metadata_buf = parh5I_get_inode_metadata_buf(group->inode);
	hid_t file_pls[2] = { 0 };

	size_t idx = 0;
	size_t decoded_size = 0;
	for (size_t i = 0; i < sizeof(file_pls) / sizeof(hid_t); i++) {
		file_pls[i] = H5Pdecode(&metadata_buf[idx]);
		PARH5G_CHECK_ERROR(file_pls[i]);
		herr_t error = H5Pencode2(file_pls[i], NULL, &decoded_size, 0);
		PARH5G_CHECK_ERROR(error)
		idx += decoded_size;
	}
	group->cpl_id = file_pls[0];
	group->apl_id = file_pls[1];
}

parh5G_group_t parh5G_open_group(parh5F_file_t file, parh5I_inode_t inode)
{
	parh5G_group_t group = calloc(1UL, sizeof(struct parh5G_group));
	group->inode = inode;
	log_debug("Deserializing group name: %s for file: %s", parh5I_get_inode_name(inode),
		  parh5F_get_file_name(file));
	parh5G_deserialize_group_metadata(group);
	group->type = H5I_GROUP;
	group->file = file;

	return group;
}

parh5G_group_t parh5G_create_group(parh5F_file_t file, const char *name, hid_t access_pl_id, hid_t create_pl_id)
{
	log_debug("Creating group: %s", name);
	parh5G_group_t group = calloc(1UL, sizeof(struct parh5G_group));
	group->type = H5I_GROUP;
	group->file = file;
	group->apl_id = H5Pcopy(access_pl_id);
	group->cpl_id = H5Pcopy(create_pl_id);
	parh5G_group_t root_group = parh5F_get_root_group(file);
	if (NULL == root_group)
		log_debug("Creating root group for: %s", name);

	group->inode = parh5I_create_inode(name, H5I_GROUP, root_group ? root_group->inode : NULL,
					   parh5F_get_parallax_db(file));
	parh5G_serialize_group_metadata(group);
	parh5I_store_inode(group->inode, parh5F_get_parallax_db(file));

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
	(void)dxpl_id;
	(void)req;

	H5I_type_t *obj_type = obj;
	parh5G_group_t parent_group = NULL;

	if (H5I_FILE == *obj_type) {
		parh5F_file_t file = (parh5F_file_t)obj;
		parent_group = parh5F_get_root_group(file);
	} else if (H5I_GROUP == *obj_type)
		parent_group = (parh5G_group_t)obj;
	else {
		log_fatal("Parent can be either a group or file instead it is: %d", *obj_type);
		_exit(EXIT_FAILURE);
	}

	/*first do a search in "parent to see if group is already there"*/
	if (parh5I_path_search(parent_group ? parent_group->inode : NULL, name, parh5G_get_parallax_db(parent_group)) >
	    0) {
		log_fatal("group %s already exists", name);
		_exit(EXIT_FAILURE);
	}
	parh5G_group_t new_group = parh5G_create_group(parent_group->file, name, gapl_id, gcpl_id);
	par_handle par_db = parh5G_get_parallax_db(new_group);
	//inform the parent
	if (!parh5I_add_pivot_in_inode(parent_group->inode, parh5I_get_inode_num(new_group->inode), name, par_db)) {
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
	H5I_type_t *obj_type = obj;
	parh5G_group_t parent_group = NULL;

	if (H5I_FILE == *obj_type) {
		parh5F_file_t file = (parh5F_file_t)obj;
		parent_group = parh5F_get_root_group(file);
	} else if (H5I_GROUP == *obj_type)
		parent_group = (parh5G_group_t)obj;
	else {
		log_fatal("Parent can be either a group or file");
		_exit(EXIT_FAILURE);
	}
	uint64_t inode_num = parh5I_path_search(parent_group->inode, name, parh5G_get_parallax_db(parent_group));

	if (inode_num) {
		parh5I_inode_t inode = parh5I_get_inode(parh5F_get_parallax_db(parent_group->file), inode_num);

		if (!inode) {
			log_fatal("Could not find inode_num: %lu I should have!", inode_num);
			_exit(EXIT_FAILURE);
		}
		return parh5G_open_group(parent_group->file, inode);
	}
	log_debug("group: %s does not exist, creating a new one", name);

	parh5G_group_t new_group = parh5G_create_group(parent_group->file, name, gapl_id, H5Pcreate(H5P_FILE_ACCESS));
	par_handle par_db = parh5G_get_parallax_db(new_group);
	//save inode to parallax
	parh5I_store_inode(new_group->inode, parh5G_get_parallax_db(parent_group));
	//inform the parent
	if (!parh5I_add_pivot_in_inode(parent_group->inode, parh5I_get_inode_num(new_group->inode), name, par_db)) {
		log_fatal("inode of parent group needs resizing XXX TODO XXX");
		_exit(EXIT_FAILURE);
	};
	// parh5I_store_inode(parent_group->inode, parh5G_get_parallax_db(parent_group));
	return new_group;
}

herr_t parh5G_get(void *obj, H5VL_group_get_args_t *group_query, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)group_query;
	(void)dxpl_id;
	(void)req;
	H5I_type_t *type = obj;
	if (H5I_FILE != *type && H5I_GROUP != *type) {
		log_fatal("Can handle only file types type is: %d", *type);
		_exit(EXIT_FAILURE);
	}
	parh5G_group_t root_group = obj;
	if (*type == H5I_FILE) {
		parh5F_file_t file = obj;
		root_group = parh5F_get_root_group(file);
	}

	if (H5VL_GROUP_GET_GCPL == group_query->op_type) {
		group_query->args.get_gcpl.gcpl_id = H5Pcopy(root_group->cpl_id);
		return PARH5_SUCCESS;
	}

	if (H5VL_GROUP_GET_INFO == group_query->op_type &&
	    H5VL_OBJECT_BY_SELF == group_query->args.get_info.loc_params.type) {
		group_query->args.get_info.ginfo->mounted = false;
		group_query->args.get_info.ginfo->storage_type = H5G_STORAGE_TYPE_UNKNOWN;
		group_query->args.get_info.ginfo->nlinks = parh5I_get_nlinks(root_group->inode);
		group_query->args.get_info.ginfo->max_corder = parh5I_get_nlinks(root_group->inode);
		log_debug("-----> Direct num links for group: %s are %u", parh5G_get_group_name(root_group),
			  parh5I_get_nlinks(root_group->inode));
		return PARH5_SUCCESS;
	}
	if (H5VL_GROUP_GET_INFO == group_query->op_type &&
	    H5VL_OBJECT_BY_NAME == group_query->args.get_info.loc_params.type) {
		log_debug("Called for group: %s and searching name (within): %s", parh5G_get_group_name(root_group),
			  group_query->args.get_info.loc_params.loc_data.loc_by_name.name);
		return PARH5_SUCCESS;
	}
	if (H5VL_GROUP_GET_INFO == group_query->op_type &&
	    H5VL_OBJECT_BY_IDX == group_query->args.get_info.loc_params.type) {
		log_debug("Called for group: %s and searching name (within): idx", parh5G_get_group_name(root_group));
	}
	if (H5VL_GROUP_GET_INFO == group_query->op_type &&
	    H5VL_OBJECT_BY_TOKEN == group_query->args.get_info.loc_params.type) {
		log_debug("Called for group: %s and searching name (within): BY_TOKEN",
			  parh5G_get_group_name(root_group));
	}

	log_fatal("Group: Sorry unimplemented function : %s XXX TODO XXX",
		  group_query->op_type == H5VL_GROUP_GET_INFO ? "GROUP_INFO" : "Unknown");
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

herr_t parh5G_close(void *obj, hid_t dxpl_id, void **req)
{
	(void)dxpl_id;
	(void)req;
	H5I_type_t *obj_type = obj;
	if (H5I_GROUP != *obj_type) {
		log_fatal("Object is not a Group object!");
		_exit(EXIT_FAILURE);
	}
	parh5G_group_t group = obj;
	parh5I_inode_t inode = parh5G_get_inode(group);
	log_debug("Closing group....%s inode_num: %lu cpl_id = %ld apl_id = %ld", parh5I_get_inode_name(inode),
		  parh5I_get_inode_num(inode), group->cpl_id, group->apl_id);

	// H5Pclose(group->cpl_id);
	// H5Pclose(group->apl_id);
	free(group->inode);
	// memset(group, 0x00, sizeof(struct parh5G_group));
	// free(group);
	return PARH5_SUCCESS;
}

inline parh5I_inode_t parh5G_get_inode(parh5G_group_t group)
{
	return group ? group->inode : NULL;
}

inline par_handle parh5G_get_parallax_db(parh5G_group_t group)
{
	return group ? parh5F_get_parallax_db(group->file) : NULL;
}

parh5I_inode_t parh5G_get_root_inode(parh5G_group_t group)
{
	parh5G_group_t root_group = parh5F_get_root_group(group->file);
	return root_group->inode;
}

parh5F_file_t parh5G_get_file(parh5G_group_t group)
{
	return group ? group->file : NULL;
}
