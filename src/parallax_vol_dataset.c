#include "parallax_vol_dataset.h"
#include <stdio.h>

void *parh5D_create(void *obj, const H5VL_loc_params_t *loc_params,
                    const char *name, hid_t lcpl_id, hid_t type_id,
                    hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id,
                    void **req) {

  (void)obj;
  (void)loc_params;
  (void)name;
  (void)lcpl_id;
  (void)type_id;
  (void)space_id;
  (void)dcpl_id;
  (void)dapl_id;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "PAR_DATASET_class from function %s in file: %s\n", __func__,
          __FILE__);
  return NULL;
}

void *parh5D_open(void *obj, const H5VL_loc_params_t *loc_params,
                  const char *name, hid_t dapl_id, hid_t dxpl_id, void **req) {

  (void)obj;
  (void)loc_params;
  (void)name;
  (void)dapl_id;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "PAR_DATASET_class from function %s in file: %s\n", __func__,
          __FILE__);
  return NULL;
}

herr_t parh5D_read(size_t count, void *dset[], hid_t mem_type_id[],
                   hid_t mem_space_id[], hid_t file_space_id[], hid_t dxpl_id,
                   void *buf[], void **req) {
  (void)count;
  (void)dset;
  (void)mem_type_id;
  (void)mem_space_id;
  (void)file_space_id;
  (void)dxpl_id;
  (void)buf;
  (void)req;
  fprintf(stderr, "PAR_DATASET_class from function %s in file: %s\n", __func__,
          __FILE__);
  return 1;
}

herr_t parh5D_write(size_t count, void *dset[], hid_t mem_type_id[],
                    hid_t mem_space_id[], hid_t file_space_id[], hid_t dxpl_id,
                    const void *buf[], void **req) {

  (void)count;
  (void)dset;
  (void)mem_type_id;
  (void)mem_space_id;
  (void)file_space_id;
  (void)dxpl_id;
  (void)buf;
  (void)req;
  fprintf(stderr, "PAR_DATASET_class from function %s in file: %s\n", __func__,
          __FILE__);
  return 1;
}

herr_t parh5D_get(void *obj, H5VL_dataset_get_args_t *args, hid_t dxpl_id,
                  void **req) {

  (void)obj;
  (void)args;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "PAR_DATASET_class from function %s in file: %s\n", __func__,
          __FILE__);
  return 1;
}

herr_t parh5D_specific(void *obj, H5VL_dataset_specific_args_t *args,
                       hid_t dxpl_id, void **req) {
  (void)obj;
  (void)args;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "PAR_DATASET_class from function %s in file: %s\n", __func__,
          __FILE__);
  return 1;
}

herr_t parh5D_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id,
                       void **req) {
  (void)obj;
  (void)args;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "PAR_DATASET_class from function %s in file: %s\n", __func__,
          __FILE__);
  return 1;
}

herr_t parh5D_close(void *dset, hid_t dxpl_id, void **req) {
  (void)dset;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "PAR_DATASET_class from function %s in file: %s\n", __func__,
          __FILE__);
  return 1;
}
