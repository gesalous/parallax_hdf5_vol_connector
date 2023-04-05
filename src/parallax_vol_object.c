#include "parallax_vol_object.h"
#include <stdio.h>

void *parh5_open(void *obj, const H5VL_loc_params_t *loc_params,
                 H5I_type_t *opened_type, hid_t dxpl_id, void **req) {
  (void)obj;
  (void)loc_params;
  (void)opened_type;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "Hello from function %s in file: %s\n", __func__, __FILE__);
  return NULL;
}
herr_t parh5_copy(void *src_obj, const H5VL_loc_params_t *loc_params1,
                  const char *src_name, void *dst_obj,
                  const H5VL_loc_params_t *loc_params2, const char *dst_name,
                  hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req) {
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
  fprintf(stderr, "Hello from function %s in file: %s\n", __func__, __FILE__);
  return 0;
}

herr_t parh5_get(void *obj, const H5VL_loc_params_t *loc_params,
                 H5VL_object_get_args_t *args, hid_t dxpl_id, void **req) {
  (void)obj;
  (void)loc_params;
  (void)args;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "Hello from function %s in file: %s\n", __func__, __FILE__);
  return 0;
}

herr_t parh5_specific(void *obj, const H5VL_loc_params_t *loc_params,
                      H5VL_object_specific_args_t *args, hid_t dxpl_id,
                      void **req) {
  (void)obj;
  (void)loc_params;
  (void)args;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "Hello from function %s in file: %s\n", __func__, __FILE__);
  return 0;
}

herr_t parh5_optional(void *obj, const H5VL_loc_params_t *loc_params,
                      H5VL_optional_args_t *args, hid_t dxpl_id, void **req) {
  (void)obj;
  (void)loc_params;
  (void)args;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "Hello from function %s in file: %s\n", __func__, __FILE__);
  return 0;
}
