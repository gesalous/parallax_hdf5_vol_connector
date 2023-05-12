#include "parallax_vol_group.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

struct par_group_handle {
  uint64_t par_group_id;
};

void *parh5G_create(void *obj, const H5VL_loc_params_t *loc_params,
                    const char *name, hid_t lcpl_id, hid_t gcpl_id,
                    hid_t gapl_id, hid_t dxpl_id, void **req) {
  (void)obj;
  (void)loc_params;
  (void)name;
  (void)lcpl_id;
  (void)gcpl_id;
  (void)gapl_id;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "PAR_GROUP_class from function %s in file: %s\n", __func__,
          __FILE__);
  return calloc(1UL, sizeof(struct par_group_handle));
}

void *parh5G_open(void *obj, const H5VL_loc_params_t *loc_params,
                  const char *name, hid_t gapl_id, hid_t dxpl_id, void **req) {
  (void)obj;
  (void)loc_params;
  (void)name;
  (void)gapl_id;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "PAR_GROUP_class from function %s in file: %s\n", __func__,
          __FILE__);
  return calloc(1UL, sizeof(struct par_group_handle));
}

herr_t parh5G_get(void *obj, H5VL_group_get_args_t *args, hid_t dxpl_id,
                  void **req) {
  (void)obj;
  (void)args;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "PAR_GROUP_class from function %s in file: %s\n", __func__,
          __FILE__);
  return 1;
}

herr_t parh5G_specific(void *obj, H5VL_group_specific_args_t *args,
                       hid_t dxpl_id, void **req) {
  (void)obj;
  (void)args;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "PAR_GROUP_class from function %s in file: %s\n", __func__,
          __FILE__);
  return 1;
}

herr_t parh5G_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id,
                       void **req) {
  (void)obj;
  (void)args;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "PAR_GROUP_class from function %s in file: %s\n", __func__,
          __FILE__);
  return 1;
}

herr_t parh5G_close(void *grp, hid_t dxpl_id, void **req) {
  (void)grp;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "PAR_GROUP_class from function %s in file: %s\n", __func__,
          __FILE__);
  return 1;
}
