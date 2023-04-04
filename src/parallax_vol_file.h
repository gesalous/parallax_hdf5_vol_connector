#ifndef PARALLAX_VOL_FILE_H
#define PARALLAX_VOL_FILE_H
#include <H5VLconnector.h>

void *parh5F_create(const char *name, unsigned flags, hid_t fcpl_id,
                    hid_t fapl_id, hid_t dxpl_id, void **req);
void *parh5F_open(const char *name, unsigned flags, hid_t fapl_id,
                  hid_t dxpl_id, void **req);
herr_t parh5F_get(void *obj, H5VL_file_get_args_t *args, hid_t dxpl_id,
                  void **req);
herr_t parh5F_specific(void *obj, H5VL_file_specific_args_t *args,
                       hid_t dxpl_id, void **req);
herr_t(parh5F_optional)(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id,
                        void **req);
herr_t parh5F_close(void *file, hid_t dxpl_id, void **req);
#endif
