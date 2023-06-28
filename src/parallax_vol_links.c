#include "parallax_vol_links.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_dataset.h"
#include "parallax_vol_file.h"
#include "parallax_vol_group.h"
#include "parallax_vol_inode.h"
#include <H5Epublic.h>
#include <H5Ipublic.h>
#include <H5Lpublic.h>
#include <H5VLconnector.h>
#include <H5VLconnector_passthru.h>
#include <H5public.h>
#include <H5version.h>
#include <assert.h>
#include <log.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

static const char *parh5L_location_type2string(const H5VL_loc_params_t *loc_params)
{
	if (loc_params->type == H5VL_OBJECT_BY_SELF)
		return "H5VL_OBJECT_BY_SELF";
	if (loc_params->type == H5VL_OBJECT_BY_NAME)
		return "H5VL_OBJECT_BY_NAME";
	if (loc_params->type == H5VL_OBJECT_BY_IDX)
		return "H5VL_OBJECT_BY_IDX";
	if (loc_params->type == H5VL_OBJECT_BY_TOKEN)
		return "H5VL_OBJECT_BY_TOKEN";
	return "Unknown";
}

static const char *parh5L_index_t2string(H5_index_t index)
{
	if (H5_INDEX_UNKNOWN == index)
		return "H5_INDEX_UNKNOWN";
	if (H5_INDEX_NAME == index)
		return "H5_INDEX_NAME";
	if (H5_INDEX_CRT_ORDER == index)
		return "H5_INDEX_CRT_ORDER";
	return "H5_INDEX_UNKNOWN";
}

static const char *parh5L_iter_order2string(H5_iter_order_t iter_order)
{
	if (H5_ITER_UNKNOWN == iter_order)
		return "H5_INDEX_UNKNOWN";
	if (H5_ITER_INC == iter_order)
		return "H5_ITER_INC";

	if (H5_ITER_DEC == iter_order)
		return "H5_ITER_DEC";
	if (H5_ITER_NATIVE == iter_order)
		return "H5_ITER_NATIVE";
	if (H5_ITER_N == iter_order)
		return "H5_ITER_N";
	return "Unknown";
}
herr_t parh5L_create(H5VL_link_create_args_t *args, void *obj, const H5VL_loc_params_t *loc_params, hid_t lcpl_id,
		     hid_t lapl_id, hid_t dxpl_id, void **req)
{
	(void)args;
	(void)obj;
	(void)loc_params;
	(void)lcpl_id;
	(void)lapl_id;
	(void)dxpl_id;
	(void)req;
	log_fatal("Unimplemented method");
	_exit(EXIT_FAILURE);
}
herr_t parh5L_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
		   const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req)
{
	(void)src_obj;
	(void)dst_obj;
	(void)loc_params1;
	(void)loc_params2;
	(void)lcpl_id;
	(void)lapl_id;
	(void)dxpl_id;
	(void)req;
	log_fatal("Unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t(parh5L_move)(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
		    const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req)
{
	(void)src_obj;
	(void)dst_obj;
	(void)loc_params1;
	(void)loc_params2;
	(void)lcpl_id;
	(void)lapl_id;
	(void)dxpl_id;
	(void)req;
	log_fatal("Unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5L_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_args_t *args, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)loc_params;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_fatal("Unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5L_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_specific_args_t *link_query,
		       hid_t dxpl_id, void **req)
{
	(void)dxpl_id;
	(void)req;

	H5I_type_t *type = obj;

	if (H5I_FILE != *type && H5I_GROUP != *type) {
		log_fatal("Sorry currently Parallax supports only files/groups");
		_exit(EXIT_FAILURE);
	}

	if (loc_params->type != H5VL_OBJECT_BY_NAME) {
		log_fatal("Sorry currently Parallax supports only H5VL_OBJECT_BY_NAME");
		assert(0);
		_exit(EXIT_FAILURE);
	}

	if (H5VL_LINK_ITER != link_query->op_type) {
		log_fatal("Sorry, currently Parallax supports only H5VL_LINK_ITER");
		_exit(EXIT_FAILURE);
	}

	log_debug("Iteration recursive? :%d index_t: %s iter order: %s loc_params: %d obj name: %s",
		  link_query->args.iterate.recursive, parh5L_index_t2string(link_query->args.iterate.idx_type),
		  parh5L_iter_order2string(link_query->args.iterate.order), loc_params->type,
		  loc_params->loc_data.loc_by_name.name);

	if (link_query->args.iterate.recursive) {
		log_fatal("Sorry does not currently support recursive look ups XXX TODO XXX");
		_exit(EXIT_FAILURE);
	}

	parh5F_file_t file = NULL;
	parh5G_group_t root_group = NULL;
	if (H5I_FILE == *type) {
		file = obj;
		root_group = parh5F_get_root_group(file);
	} else {
		root_group = obj;
		file = parh5G_get_file(root_group);
	}

	parh5I_inode_t root_inode = parh5G_get_inode(root_group);
	uint64_t obj_cnt = parh5I_get_nlinks(root_inode);
	char **objs = calloc(obj_cnt, sizeof(char *));
	parh5I_get_children_names(root_inode, objs, &obj_cnt, file);
	for (size_t obj_id = 0; obj_id < obj_cnt; obj_id++) {
		// H5L_info2_t link_info = { .type = H5L_TYPE_HARD, .corder = false, .cset = H5T_CSET_ASCII };
		// herr_t error = link_query->args.iterate.op(objs.oid_list[obj_id], name, NULL,
		// 					   link_query->args.iterate.op_data);
		log_debug("Calling callback for name: %s", objs[obj_id]);
		herr_t error = link_query->args.iterate.op(-1, (const char *)objs[obj_id], NULL,
							   link_query->args.iterate.op_data);

		if (error >= 0)
			continue;
		log_fatal("Callback failed");
		_exit(EXIT_FAILURE);
	}

	// Get the error stack
	// hid_t error_stack = H5Eget_current_stack();

	// Get the number of error messages in the stack
	// size_t error_count = H5Eget_num(error_stack);

	// Iterate over the error messages in the stack
	// for (size_t i = 0; i < error_count; i++) {
	// Get the error message
	// char error_msg[256] = { 0 };
	// H5E_type_t error_type;
	// H5Eget_msg(i, &error_type, error_msg, sizeof(error_msg));

	// Print the error message
	// 		log_debug("Error no %zu: %s type is %s", i, error_msg,
	// 			  error_type == H5E_MAJOR ? "MAjor" : "Minor");
	// 	}

	// 	H5Eclose_stack(error_stack);
	// 	_exit(EXIT_FAILURE);
	// }
	free(objs);
	return PARH5_SUCCESS;
	// log_fatal("Unimplemented method. is it a file? %s location type: %s", H5I_FILE == *type ? "YES" : "NO",
	// 	  parh5L_location_type2string(loc_params));
	// _exit(EXIT_FAILURE);
}

herr_t parh5L_optional(void *obj, const H5VL_loc_params_t *loc_params, H5VL_optional_args_t *args, hid_t dxpl_id,
		       void **req)
{
	(void)obj;
	(void)loc_params;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_fatal("Unimplemented method");
	_exit(EXIT_FAILURE);
}
