#ifndef PARALLAX_VOL_ATTRIBUTE_H
#define PARALLAX_VOL_ATTRIBUTE_H
#include <H5VLconnector.h>

/* H5A routines */
void *parh5A_create(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name, hid_t type_id,
		    hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req);

void *parh5A_open(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name, hid_t aapl_id, hid_t dxpl_id,
		  void **req);
herr_t parh5A_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req);
herr_t parh5A_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req);
herr_t parh5A_get(void *obj, H5VL_attr_get_args_t *args, hid_t dxpl_id, void **req);
herr_t parh5A_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_args_t *args, hid_t dxpl_id,
		       void **req);
herr_t parh5A_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
herr_t parh5A_close(void *attr, hid_t dxpl_id, void **req);
#endif
