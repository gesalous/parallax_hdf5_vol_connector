#ifndef PARALLAX_VOL_INTROSPECT_H
#define PARALLAX_VOL_INTROSPECT_H
#include <H5VLconnector.h>
herr_t parh5_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl,
                          const struct H5VL_class_t **conn_cls);
herr_t parh5_get_cap_flags(const void *info, uint64_t *cap_flags);
herr_t parh5_opt_query(void *obj, H5VL_subclass_t cls, int opt_type,
                       uint64_t *flags);
#endif
