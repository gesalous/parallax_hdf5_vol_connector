#include "parallax_vol_object.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_dataset.h"
#include "parallax_vol_file.h"
#include "parallax_vol_group.h"
#include "parallax_vol_inode.h"
#include <H5Ipublic.h>
#include <H5VLconnector.h>
#include <H5VLconnector_passthru.h>
#include <assert.h>
#include <log.h>
#include <signal.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void *parh5_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
	(void)opened_type;
	(void)dxpl_id;
	(void)req;
	H5I_type_t *type = obj;
	if (*type != H5I_FILE && *type != H5I_GROUP) {
		log_fatal("Sorry currently PARALLAX supports only FILEs/GROUPS in open object XXX TODO XXX");
		_exit(EXIT_FAILURE);
	}
	parh5F_file_t file = NULL;
	parh5G_group_t root_group = NULL;
	if (H5I_FILE == *type) {
		log_debug("Obj is a file");
		file = obj;
		root_group = parh5F_get_root_group(file);
		log_debug("Obj is PARALLAX FILE named: %s in file: %s", parh5F_get_file_name(file),
			  parh5G_get_group_name(root_group));
	} else {
		root_group = obj;
		file = parh5G_get_file(root_group);
		log_debug("Obj is PARALLAX GROUP named: %s in file: %s", parh5G_get_group_name(root_group),
			  parh5F_get_file_name(file));
	}

	if (loc_params->type != H5VL_OBJECT_BY_NAME) {
		log_fatal("Sorry currently Parallax supports only locating objects by name");
		_exit(EXIT_FAILURE);
	}
	const char *name = loc_params->loc_data.loc_by_name.name;
	parh5I_inode_t inode = parh5G_get_inode(root_group);
	log_debug("Searching object: %s in file: %s", name, parh5F_get_file_name(file));
	void *ret_obj = parh5I_find_object(inode, name, opened_type, file);

	if (NULL == ret_obj) {
		log_fatal("Nothing found with the name: %s", name);
		_exit(EXIT_FAILURE);
	}

	if (*opened_type == H5I_DATASET)
		log_debug("Found dataset with name: %s", name);
	if (*opened_type == H5I_GROUP)
		log_debug("Found group with name: %s", name);
	return ret_obj;
}
herr_t parh5_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, const char *src_name, void *dst_obj,
		  const H5VL_loc_params_t *loc_params2, const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id,
		  hid_t dxpl_id, void **req)
{
	(void)src_obj;
	(void)loc_params1;
	(void)src_name;
	(void)dst_obj;
	(void)loc_params2;
	(void)dst_name;
	(void)ocpypl_id;
	(void)lcpl_id;
	(void)dxpl_id;
	(void)req;
	log_fatal("Unimplemented method Sorry");
	_exit(EXIT_FAILURE);
}

herr_t parh5_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_args_t *args, hid_t dxpl_id,
		 void **req)
{
	(void)obj;
	(void)loc_params;
	(void)args;
	(void)dxpl_id;
	(void)req;
	H5I_type_t *type = obj;
	if (*type != H5I_GROUP && *type != H5I_DATASET && *type != H5I_FILE) {
		log_fatal("Sorry, unsupported type: %d", *type);
		_exit(EXIT_FAILURE);
	}
	if (args->op_type != H5VL_OBJECT_GET_NAME && args->op_type != H5VL_OBJECT_GET_FILE) {
		log_fatal("Sorry currently support only H5VL_OBJECT_GET_NAME op_type is %d", args->op_type);
		_exit(EXIT_FAILURE);
	}

	if (args->op_type == H5VL_OBJECT_GET_FILE) {
		switch (*type) {
		case H5I_FILE:
			*args->args.get_file.file = obj;
			return PARH5_SUCCESS;
		case H5I_DATASET:;
			parh5F_file_t file = parh5D_get_file(obj);
			*args->args.get_file.file = file;
			return PARH5_SUCCESS;
		case H5I_GROUP:
			*args->args.get_file.file = parh5G_get_file(obj);
			return PARH5_SUCCESS;

		default:
			log_fatal("Cannot handle it op_type is: %d", *type);
			_exit(EXIT_FAILURE);
		}
	}

	const char *name = NULL;
	if (H5I_FILE == *type)
		name = parh5F_get_file_name(obj);
	if (H5I_GROUP == *type)
		name = parh5G_get_group_name(obj);
	if (H5I_DATASET == *type)
		name = parh5D_get_dataset_name(obj);

	*args->args.get_name.name_len = strlen(name) + 1;
	if (args->args.get_name.buf_size < strlen(name) + 1 || NULL == args->args.get_name.buf)
		return PARH5_SUCCESS;
	memcpy(args->args.get_name.buf, name, strlen(name) + 1);
	return PARH5_SUCCESS;
}

herr_t parh5_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_specific_args_t *args, hid_t dxpl_id,
		      void **req)
{
	(void)obj;
	(void)loc_params;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_fatal("Unimplemented method Sorry");
	_exit(EXIT_FAILURE);
}

herr_t parh5_optional(void *obj, const H5VL_loc_params_t *loc_params, H5VL_optional_args_t *args, hid_t dxpl_id,
		      void **req)
{
	(void)obj;
	(void)loc_params;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_fatal("Unimplemented method Sorry");
	_exit(EXIT_FAILURE);
}
