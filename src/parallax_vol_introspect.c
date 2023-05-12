#include "parallax_vol_introspect.h"
#include <stdio.h>

herr_t parh5_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl,
                          const struct H5VL_class_t **conn_cls) {
  (void)obj;
  (void)lvl;
  (void)conn_cls;
  fprintf(stderr,
          "PAR_INTROSPECT_class from function: %s, in file: %s, and line: %d\n",
          __func__, __FILE__, __LINE__);
  return 1;
}

herr_t parh5_get_cap_flags(const void *info, uint64_t *cap_flags) {
  (void)info;
  (void)cap_flags;
  fprintf(stderr,
          "PAR_INTROSPECT_class from function: %s, in file: %s, and line: %d\n",
          __func__, __FILE__, __LINE__);
  return 1;
}

herr_t parh5_opt_query(void *obj, H5VL_subclass_t cls, int opt_type,
                       uint64_t *flags) {
  (void)obj;
  (void)cls;
  (void)opt_type;
  (void)flags;
  fprintf(stderr,
          "PAR_INTROSPECT_class from function: %s, in file: %s, and line: %d\n",
          __func__, __FILE__, __LINE__);
  return 1;
}
