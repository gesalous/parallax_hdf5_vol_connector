#include "parallax_vol_attribute.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_dataset.h"
#include <H5Ipublic.h>
#include <log.h>
#include <stdlib.h>
#include <unistd.h>

static const char *parh5A_HPItype_to_string(H5I_type_t type)
{
	if (H5I_UNINIT == type)
		return "H5I_UNINIT";
	if (H5I_BADID == type)
		return "H5I_BADID";
	char *types[H5I_NTYPES] = { "Error",	       "H5I_FILE",	"H5I_GROUP",	   "H5I_DATATYPE",
				    "H5I_DATASPACE",   "H5I_DATASET",	"H5I_MAP",	   "H5I_ATTR",
				    "H5I_VFL",	       "H5I_VOL",	"H5I_GENPROP_CLS", "H5I_GENPROP_LST",
				    "H5I_ERROR_CLASS", "H5I_ERROR_MSG", "H5I_ERROR_STACK", "H5I_SPACE_SEL_ITER",
				    "H5I_EVENTSET" };
	return types[type];
}

void *parh5A_create(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name, hid_t type_id,
		    hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)loc_params;
	(void)attr_name;
	(void)type_id;
	(void)space_id;
	(void)acpl_id;
	(void)aapl_id;
	(void)dxpl_id;
	(void)req;
	log_debug("Sorry unimplemented method");
	_exit(EXIT_FAILURE);
}

void *parh5A_open(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name, hid_t aapl_id, hid_t dxpl_id,
		  void **req)
{
	(void)obj;
	(void)loc_params;
	(void)attr_name;
	(void)aapl_id;
	(void)dxpl_id;
	(void)req;
	log_debug("Sorry unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5A_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req)
{
	(void)attr;
	(void)mem_type_id;
	(void)buf;
	(void)dxpl_id;
	(void)req;
	log_debug("Sorry unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5A_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req)
{
	(void)attr;
	(void)mem_type_id;
	(void)buf;
	(void)dxpl_id;
	(void)req;
	log_debug("Sorry unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5A_get(void *obj, H5VL_attr_get_args_t *args, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_debug("Sorry unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5A_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_args_t *args, hid_t dxpl_id,
		       void **req)
{
	H5I_type_t *obj_type = obj;
	if (H5I_DATASET == *obj_type && loc_params->obj_type == H5I_DATASET) {
		parh5D_dataset_t dataset = (parh5D_dataset_t)obj;
		log_debug("Attribute for dataset");
	} else {
		log_fatal("Sorry handling attributes only for dataset at the moment");
		_exit(EXIT_FAILURE);
	}
	(void)obj;
	(void)loc_params;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_debug("Requested %s", parh5A_HPItype_to_string(loc_params->obj_type));
	log_debug("Sorry unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5A_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_debug("Sorry unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5A_close(void *attr, hid_t dxpl_id, void **req)
{
	(void)attr;
	(void)dxpl_id;
	(void)req;
	log_debug("Sorry unimplemented method");
	_exit(EXIT_FAILURE);
}
