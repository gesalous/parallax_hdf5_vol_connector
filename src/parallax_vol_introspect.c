#include "parallax_vol_introspect.h"
#include "parallax_vol_connector.h"
#include <log.h>
#include <stdio.h>

#define PARH5_OPT_QUERY_OUT_TYPE uint64_t
#define PARH5_OPT_QUERY_SUPPORTED H5VL_OPT_QUERY_SUPPORTED
#define PARH5_OPT_QUERY_READ_DATA H5VL_OPT_QUERY_READ_DATA
#define PARH5_OPT_QUERY_WRITE_DATA H5VL_OPT_QUERY_WRITE_DATA
#define PARH5_OPT_QUERY_QUERY_METADATA H5VL_OPT_QUERY_QUERY_METADATA
#define PARH5_OPT_QUERY_MODIFY_METADATA H5VL_OPT_QUERY_MODIFY_METADATA
#define PARH5_OPT_QUERY_COLLECTIVE H5VL_OPT_QUERY_COLLECTIVE
#define PARH5_OPT_QUERY_NO_ASYNC H5VL_OPT_QUERY_NO_ASYNC
#define PARH5_OPT_QUERY_MULTI_OBJ H5VL_OPT_QUERY_MULTI_OBJ

herr_t parh5_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const struct H5VL_class_t **conn_cls)
{
	(void)obj;
	(void)lvl;
	(void)conn_cls;
	fprintf(stderr, "PAR_INTROSPECT_class from function: %s, in file: %s, and line: %d\n", __func__, __FILE__,
		__LINE__);
	return 1;
}

herr_t parh5_get_cap_flags(const void *info, uint64_t *cap_flags)
{
	(void)info;
	(void)cap_flags;
	fprintf(stderr, "PAR_INTROSPECT_class from function: %s, in file: %s, and line: %d\n", __func__, __FILE__,
		__LINE__);
	return 1;
}

herr_t parh5_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, uint64_t *supported)
{
	(void)cls;
	log_debug("HDF5 is examing PARALLAX VOL plugin capabilities...");
	if (!obj) {
		log_warn("obj not provided!");
		return PARH5_FAILURE;
	}

	if (!supported) {
		log_warn("supported vector not provided!");
		return PARH5_FAILURE;
	}

	/* Check operation type */
	switch (opt_type) {
	case H5VL_MAP_CREATE:
		log_debug("Checking H5VL_MAP_CREATE capability");
		*supported = PARH5_OPT_QUERY_SUPPORTED | PARH5_OPT_QUERY_MODIFY_METADATA;
		break;

	case H5VL_MAP_OPEN:
		log_debug("H5VL_MAP_OPEN capability");
		*supported = PARH5_OPT_QUERY_SUPPORTED | PARH5_OPT_QUERY_QUERY_METADATA;
		break;

	case H5VL_MAP_GET_VAL:
		log_debug("Checking H5VL_MAP_GET_VAL capability");
		*supported = PARH5_OPT_QUERY_SUPPORTED | PARH5_OPT_QUERY_READ_DATA | PARH5_OPT_QUERY_NO_ASYNC;
		break;

	case H5VL_MAP_EXISTS:
		log_debug("Checking H5VL_MAP_EXISTS capability");
		*supported = PARH5_OPT_QUERY_SUPPORTED | PARH5_OPT_QUERY_READ_DATA | PARH5_OPT_QUERY_NO_ASYNC;
		break;

		*supported = PARH5_OPT_QUERY_SUPPORTED | PARH5_OPT_QUERY_READ_DATA | PARH5_OPT_QUERY_NO_ASYNC;
		break;

	case H5VL_MAP_GET:
		log_debug("Checking H5VL_MAP_GET capability");
		*supported = PARH5_OPT_QUERY_SUPPORTED | PARH5_OPT_QUERY_QUERY_METADATA | PARH5_OPT_QUERY_NO_ASYNC;
		break;

	case H5VL_MAP_SPECIFIC:
		log_debug("Checking H5VL_MAP_SPECIFIC capability");
		*supported = PARH5_OPT_QUERY_SUPPORTED | PARH5_OPT_QUERY_QUERY_METADATA |
			     PARH5_OPT_QUERY_MODIFY_METADATA | PARH5_OPT_QUERY_NO_ASYNC;
		break;

	case H5VL_MAP_CLOSE:
		log_debug("Checking H5VL_MAP_CLOSE capability");
		*supported = PARH5_OPT_QUERY_SUPPORTED;
		break;
	default:
		log_debug("Checking an option Parallax does not supports");
		/* Not supported */
		*supported = 0;
		break;
	}

	return PARH5_SUCCESS;
}
