#ifndef PARALLAX_VOL_GROUP_H
#define PARALLAX_VOL_GROUP_H
#include <H5VLconnector.h>
void *parh5G_create(void *obj, const H5VL_loc_params_t *loc_params,
                    const char *name, hid_t lcpl_id, hid_t gcpl_id,
                    hid_t gapl_id, hid_t dxpl_id, void **req);
void *parh5G_open(void *obj, const H5VL_loc_params_t *loc_params,
                  const char *name, hid_t gapl_id, hid_t dxpl_id, void **req);
herr_t parh5G_get(void *obj, H5VL_group_get_args_t *args, hid_t dxpl_id,
                  void **req);
herr_t parh5G_specific(void *obj, H5VL_group_specific_args_t *args,
                       hid_t dxpl_id, void **req);
herr_t parh5G_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id,
                       void **req);
herr_t parh5G_close(void *grp, hid_t dxpl_id, void **req);
#endif
