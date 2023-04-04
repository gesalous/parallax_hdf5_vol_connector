#ifndef PARALLAX_VOL_OBJECT_H
#define PARALLAX_VOL_OBJECT_H
#include <H5VLconnector.h>
void *parh5_open(void *obj, const H5VL_loc_params_t *loc_params,
                 H5I_type_t *opened_type, hid_t dxpl_id, void **req);
herr_t parh5_copy(void *src_obj, const H5VL_loc_params_t *loc_params1,
                  const char *src_name, void *dst_obj,
                  const H5VL_loc_params_t *loc_params2, const char *dst_name,
                  hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req);
herr_t parh5_get(void *obj, const H5VL_loc_params_t *loc_params,
                 H5VL_object_get_args_t *args, hid_t dxpl_id, void **req);
herr_t parh5_specific(void *obj, const H5VL_loc_params_t *loc_params,
                      H5VL_object_specific_args_t *args, hid_t dxpl_id,
                      void **req);
herr_t parh5_optional(void *obj, const H5VL_loc_params_t *loc_params,
                      H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
#endif
