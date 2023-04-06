#include "parallax_vol_file.h"
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
struct par_file_handle {
  uint64_t uuid;
};

void *parh5F_create(const char *name, unsigned flags, hid_t fcpl_id,
                    hid_t fapl_id, hid_t dxpl_id, void **req) {
  (void)name;
  (void)flags;
  (void)fcpl_id;
  (void)fapl_id;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr,
          "Hello from function: %s, in file: %s, and line: %d name is: %s\n",
          __func__, __FILE__, __LINE__, name);
  fprintf(stderr, "name: %s", name);
  return calloc(1UL, sizeof(struct par_file_handle));
}

void *parh5F_open(const char *name, unsigned flags, hid_t fapl_id,
                  hid_t dxpl_id, void **req) {
  (void)name;
  (void)flags;
  (void)fapl_id;
  (void)dxpl_id;
  (void)req;

  fprintf(stderr, "Hello from function %s, in file: %s, and line: %d\n",
          __func__, __FILE__, __LINE__);
  return calloc(1UL, sizeof(struct par_file_handle));
}

herr_t parh5F_get(void *obj, H5VL_file_get_args_t *args, hid_t dxpl_id,
                  void **req) {
  (void)obj;
  (void)args;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "Hello from function %s in file: %s\n", __func__, __FILE__);
  return 1;
}

herr_t parh5F_specific(void *obj, H5VL_file_specific_args_t *args,
                       hid_t dxpl_id, void **req) {
  (void)obj;
  (void)args;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "Hello from function %s in file: %s\n", __func__, __FILE__);
  return 1;
}

herr_t(parh5F_optional)(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id,
                        void **req) {
  (void)obj;
  (void)args;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "Hello from function %s in file: %s\n", __func__, __FILE__);
  return 1;
}

herr_t parh5F_close(void *file, hid_t dxpl_id, void **req) {
  (void)file;
  (void)dxpl_id;
  (void)req;
  fprintf(stderr, "Hello from function %s in file: %s\n", __func__, __FILE__);
  return 1;
}
