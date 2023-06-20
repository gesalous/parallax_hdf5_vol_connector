#include "parallax_vol_object.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_dataset.h"
#include "parallax_vol_group.h"
#include <H5Ipublic.h>
#include <log.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void *parh5_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)loc_params;
	(void)opened_type;
	(void)dxpl_id;
	(void)req;
	log_fatal("Unimplemented method Sorry");
	_exit(EXIT_FAILURE);
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
	if (*type != H5I_GROUP && *type != H5I_DATASET) {
		log_fatal("Sorry, unsupported type");
		_exit(EXIT_FAILURE);
	}
	if (args->op_type != H5VL_OBJECT_GET_NAME) {
		log_fatal("Sorry currently support only H5VL_OBJECT_GET_NAME");
		_exit(EXIT_FAILURE);
	}
	const char *name = *type == H5I_GROUP ? parh5G_get_group_name((parh5G_group_t)obj) :
						parh5D_get_dataset_name((parh5D_dataset_t)obj);
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
