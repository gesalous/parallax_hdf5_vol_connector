#ifndef PARALLAX_VOL_FILE_H
#define PARALLAX_VOL_FILE_H
#include "parallax_vol_group.h"
#include <H5VLconnector.h>
#include <parallax/structures.h>
typedef struct parh5G_group *parh5G_group_t;

/*VOL-plugin specific*/
void *parh5F_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
void *parh5F_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
herr_t parh5F_get(void *obj, H5VL_file_get_args_t *args, hid_t dxpl_id, void **req);
herr_t parh5F_specific(void *obj, H5VL_file_specific_args_t *args, hid_t dxpl_id, void **req);
herr_t(parh5F_optional)(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
herr_t parh5F_close(void *file, hid_t dxpl_id, void **req);

/*non VOL-plugin specific*/
typedef struct parh5F_file *parh5F_file_t;
const char *parh5F_get_file_name(parh5F_file_t file);
par_handle parh5F_get_parallax_db(parh5F_file_t file);

parh5G_group_t parh5F_get_root_group(parh5F_file_t file);

#endif
