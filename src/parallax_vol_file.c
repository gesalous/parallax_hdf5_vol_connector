#include "parallax_vol_file.h"
#include "parallax/structures.h"
#include "parallax_vol_connector.h"
#include "uthash.h"
#include <H5Fpublic.h>
#include <H5Ppublic.h>
#include <H5Tpublic.h>
#include <assert.h>
#include <hdf5.h>
#include <log.h>
#include <parallax/parallax.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

struct par_file_handle {
  par_handle db;
};

static const char *parh5F_flags2s(unsigned flags) {
  if (flags & H5F_ACC_TRUNC)
    return "H5F_ACC_TRUNC";
  if (flags & H5F_ACC_EXCL)
    return "H5F_ACC_EXCL ";
  if (flags & H5F_ACC_DEBUG)
    return "H5F_ACC_DEBUG ";
  return "NULL flags";
}

void *parh5F_create(const char *name, unsigned flags, hid_t fcpl_id,
                    hid_t fapl_id, hid_t dxpl_id, void **req) {
  (void)name;
  (void)flags;
  (void)fcpl_id;
  (void)fapl_id;
  (void)dxpl_id;
  (void)req;
  log_debug("PAR_FILE_class name is: %s\n", name);
  log_debug("PAR_FILE_class name: %s flags %s", name, parh5F_flags2s(flags));

  int idx = 0;
  // Print the name of the property
  size_t nprops = 0;
  H5Pget_nprops(fapl_id, &nprops);
  hid_t class_id = H5Pget_class(fapl_id);
  log_debug("%s number of properties: %zu", H5Pget_class_name(class_id),
            nprops);
  H5Piterate(fcpl_id, &idx, parh5_property_list_iterator, NULL);
  log_debug("***\n\n");
  idx = 0;
  H5Pget_nprops(fcpl_id, &nprops);
  class_id = H5Pget_class(fcpl_id);
  log_debug("%s number of properties: %zu", H5Pget_class_name(class_id),
            nprops);
  H5Piterate(fapl_id, &idx, parh5_property_list_iterator, NULL);
  log_debug("***\n\n");
  idx = 0;
  H5Pget_nprops(dxpl_id, &nprops);
  class_id = H5Pget_class(dxpl_id);
  log_debug("%s number of properties: %zu", H5Pget_class_name(class_id),
            nprops);
  H5Piterate(dxpl_id, &idx, parh5_property_list_iterator, NULL);
  _exit(EXIT_FAILURE);
  return calloc(1UL, sizeof(struct par_file_handle));
}

void *parh5F_open(const char *name, unsigned flags, hid_t fapl_id,
                  hid_t dxpl_id, void **req) {
  (void)name;
  (void)flags;
  (void)fapl_id;
  (void)dxpl_id;
  (void)req;

  log_debug("PAR_FILE_class name is %s", name);
  return calloc(1UL, sizeof(struct par_file_handle));
}

herr_t parh5F_get(void *obj, H5VL_file_get_args_t *args, hid_t dxpl_id,
                  void **req) {
  (void)obj;
  (void)args;
  (void)dxpl_id;
  (void)req;
  log_debug("PAR_FILE_class\n");
  return 1;
}

herr_t parh5F_specific(void *obj, H5VL_file_specific_args_t *args,
                       hid_t dxpl_id, void **req) {
  (void)obj;
  (void)args;
  (void)dxpl_id;
  (void)req;
  log_debug("PAR_FILE_class from function\n");
  return 1;
}

herr_t(parh5F_optional)(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id,
                        void **req) {
  (void)obj;
  (void)args;
  (void)dxpl_id;
  (void)req;
  log_debug("PAR_FILE_class from function");
  return 1;
}

herr_t parh5F_close(void *file, hid_t dxpl_id, void **req) {
  (void)file;
  (void)dxpl_id;
  (void)req;
  log_debug("PAR_FILE_class from function");
  return 1;
}
