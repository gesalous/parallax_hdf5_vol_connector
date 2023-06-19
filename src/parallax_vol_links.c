#include "parallax_vol_links.h"
#include <log.h>
#include <stdlib.h>
#include <unistd.h>
herr_t parh5L_create(H5VL_link_create_args_t *args, void *obj, const H5VL_loc_params_t *loc_params, hid_t lcpl_id,
		     hid_t lapl_id, hid_t dxpl_id, void **req)
{
	log_fatal("Unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5L_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
		   const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req)
{
	log_fatal("Unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t(parh5L_move)(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
		    const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req)
{
	log_fatal("Unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5L_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_args_t *args, hid_t dxpl_id, void **req)
{
	log_fatal("Unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5L_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_specific_args_t *args, hid_t dxpl_id,
		       void **req)
{
	log_fatal("Unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5L_optional(void *obj, const H5VL_loc_params_t *loc_params, H5VL_optional_args_t *args, hid_t dxpl_id,
		       void **req)
{
	log_fatal("Unimplemented method");
	_exit(EXIT_FAILURE);
}
