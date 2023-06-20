#include "parallax_vol_file.h"
#include "H5VLconnector.h"
#include "H5public.h"
#include "parallax/structures.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_group.h"
#include "parallax_vol_inode.h"
#include "uthash.h"
#include <H5Fpublic.h>
#include <H5Ipublic.h>
#include <H5Ppublic.h>
#include <H5Tpublic.h>
#include <assert.h>
#include <hdf5.h>
#include <log.h>
#include <parallax/parallax.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
typedef struct parh5G_group *parh5G_group_t;

struct parh5F_file {
	H5I_type_t obj_type;
	const char *name;
	parh5G_group_t root_group;
	par_handle db;
};

static hid_t parh5F_get_fcpl(parh5F_file_t file)
{
	parh5G_group_t root_group = parh5F_get_root_group(file);
	hid_t fcpl_id = parh5G_get_cpl(root_group);
	if (fcpl_id < 0) {
		log_fatal("Could not retrieve fcpl for file: %s", parh5F_get_file_name(file));
		_exit(EXIT_FAILURE);
	}
	hid_t cpl = H5Pcopy(fcpl_id);
	if (cpl < 0) {
		log_fatal("Failed to copy cpl for file: %s", parh5F_get_file_name(file));
		_exit(EXIT_FAILURE);
	}
	return cpl;
}

parh5G_group_t parh5F_get_root_group(parh5F_file_t file)
{
	return file ? file->root_group : NULL;
}

static void parh5F_print_pl(hid_t fapl_id)
{
	H5Piterate(fapl_id, NULL, parh5_property_list_iterator, NULL);
}

inline const char *parh5F_get_file_name(parh5F_file_t file)
{
	return file ? file->name : NULL;
}

inline par_handle parh5F_get_parallax_db(parh5F_file_t file)
{
	return file ? file->db : NULL;
}

static parh5F_file_t parh5F_new_file(const char *file_name, enum par_db_initializers open_flag, hid_t fapl_id,
				     hid_t fcpl_id)
{
	par_db_options db_options = { .volume_name = PARALLAX_VOLUME,
				      .create_flag = open_flag,
				      .db_name = file_name,
				      .options = par_get_default_options() };
	db_options.options[LEVEL0_SIZE].value = PARALLAX_L0_SIZE;
	db_options.options[GROWTH_FACTOR].value = PARALLAX_GROWTH_FACTOR;
	db_options.options[PRIMARY_MODE].value = 1;

	parh5F_file_t file = calloc(1UL, sizeof(struct parh5F_file));
	const char *error_message = NULL;
	file->obj_type = H5I_FILE;
	file->db = par_open(&db_options, &error_message);
	if (error_message)
		log_debug("Parallax says: %s", error_message);

	if (file->db == NULL && error_message) {
		log_fatal("Error uppon opening the DB, error %s", error_message);
		_exit(EXIT_FAILURE);
	}
	file->name = strdup(file_name);

	/*Check it the root inode exists*/
	parh5I_inode_t root_inode = parh5I_get_inode(file->db, 1);
	if (root_inode) {
		file->root_group = parh5G_open_group(file, root_inode);
		log_debug("Opened root group for file: %s", file_name);
		return file;
	}
	if (0 == fcpl_id)
		fcpl_id = H5Pcreate(H5P_FILE_CREATE);

	if (0 == fapl_id)
		fcpl_id = H5Pcreate(H5P_FILE_ACCESS);

	file->root_group = parh5G_create_group(file, "-ROOT-", fapl_id, fcpl_id);
	log_debug("Created root group for file: %s", file_name);
	return file;
}

static void parh5F_close_parallax_db(parh5F_file_t file)
{
	const char *error = par_close(file->db);
	if (error == NULL)
		return;
	log_fatal("Failed to file: %s hosted in parallax db: %s", parh5F_get_file_name(file),
		  par_get_db_name(parh5F_get_parallax_db(file), &error));
	_exit(EXIT_FAILURE);
}

static const char *parh5F_flags2s(unsigned flags)
{
	if (flags & H5F_ACC_TRUNC)
		return "H5F_ACC_TRUNC";
	if (flags & H5F_ACC_EXCL)
		return "H5F_ACC_EXCL ";
	if (flags & H5F_ACC_DEBUG)
		return "H5F_ACC_DEBUG ";
	return "NULL flags";
}

void *parh5F_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
	(void)flags;
	(void)fcpl_id;
	(void)fapl_id;
	(void)dxpl_id;
	(void)req;
	char *yes = "YES";
	char *no = "NO";
	hbool_t want_posix_fd = false;
	hbool_t use_file_locking = false;
	hbool_t ignore_disabled_file_locks = false;
	if (H5Pget(fapl_id, PARH5_WANT_POSIX_FD, &want_posix_fd) < 0)
		log_warn("property %s not found", PARH5_WANT_POSIX_FD);

	if (H5Pget(fapl_id, PARH5_USE_FILE_LOCKING, &use_file_locking) < 0)
		log_warn("property %s not found", PARH5_USE_FILE_LOCKING);

	if (H5Pget(fapl_id, PARH5_IGNORE_DISABLED_FILE_LOCKS, &ignore_disabled_file_locks) < 0)
		log_warn("property %s not found", PARH5_USE_FILE_LOCKING);

	log_debug(
		"creating new file: %s flags %s Does it want a POSIX FD?: %s Does it want to use file locking? %s Does it want to ignore disabled file locks? %s",
		name, parh5F_flags2s(flags), want_posix_fd ? yes : no, use_file_locking ? yes : no,
		ignore_disabled_file_locks ? yes : no);
	return parh5F_new_file(name, PAR_CREATE_DB, fapl_id, fcpl_id);
}

void *parh5F_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
	(void)name;
	(void)flags;
	(void)fapl_id;
	(void)dxpl_id;
	(void)req;

	log_debug("Opening file name: %s flags %s", name, parh5F_flags2s(flags));
	return parh5F_new_file(name, PAR_CREATE_DB, fapl_id, 0);
}

static const char *parh5F_type_to_string(int type)
{
	if (type == -2)
		return "ALL OBJECTS";

	if (type == H5O_TYPE_UNKNOWN)
		return "H50_TYPE_UNKNOWN";

	if (type == H5O_TYPE_GROUP)
		return "H50_TYPE_GROUP";

	if (type == H5O_TYPE_DATASET)
		return "H5O_TYPE_DATASET";

	if (type == H5O_TYPE_NAMED_DATATYPE)
		return "H5O_TYPE_NAMED_DATATYPE";

	if (type == H5O_TYPE_MAP)
		return "H5O_TYPE_MAP";

	return "What?";
}

struct parh5F_obj_data {
	size_t max_objs;
	hid_t *oid_list;
	size_t *obj_count;
	bool is_count;
};

static herr_t parhh5F_get_obj_ids_callback(hid_t id, void *udata)
{
	struct parh5F_obj_data *id_data = (struct parh5F_obj_data *)udata;
	ssize_t connector_name_len = PARALLAX_VOL_CONNECTOR_NAME_SIZE;
	char connector_name[PARALLAX_VOL_CONNECTOR_NAME_SIZE] = { 0 };
	/* Ensure that the ID represents a DAOS VOL object */
	connector_name_len = H5VLget_connector_name(id, connector_name, connector_name_len);
	log_debug("--->Connector name is: %s", connector_name);

	/* H5VLget_connector_name should only fail for IDs that don't represent VOL objects */
	if (connector_name_len < 0) {
		log_fatal("Could not get connector object");
		_exit(EXIT_FAILURE);
	}

	if (strcmp(connector_name, PARALLAX_VOL_CONNECTOR_NAME)) {
		log_warn("Not a Parallax object vol is %s", connector_name);
		return PARH5_SUCCESS;
	}
	H5I_type_t *type = H5VLobject(id);
	if (NULL == type) {
		log_fatal("can't retrieve VOL object for ID");
		_exit(EXIT_FAILURE);
	}
	if (*type != H5I_GROUP && *type != H5I_DATASET) {
		log_fatal("Parallax object should be either a group or a dataset");
		_exit(EXIT_FAILURE);
	}

	if (id_data->is_count) {
		(*id_data->obj_count)++;
		return H5_ITER_CONT;
	}

	if (*id_data->obj_count < id_data->max_objs) {
		id_data->oid_list[(*id_data->obj_count)++] = id;
		return H5_ITER_CONT;
	}
	return H5_ITER_STOP;
}

static void parh5F_iterate_objs(unsigned obj_types, H5I_iterate_func_t filter, void *data)
{
	if (obj_types & H5F_OBJ_FILE) {
		log_debug("Iterating files...");
		if (H5Iiterate(H5I_FILE, filter, data) < 0) {
			log_fatal("failed to iterate over file's open file IDs");
			_exit(EXIT_FAILURE);
		}
	}
	if (obj_types & H5F_OBJ_GROUP) {
		log_debug("Iterating groups...");
		if (H5Iiterate(H5I_GROUP, filter, data) < 0) {
			log_fatal("failed to iterate over file's open file IDs");
			_exit(EXIT_FAILURE);
		}
	}
	if (obj_types & H5F_OBJ_DATASET) {
		log_debug("Iterating datasets...");
		if (H5Iiterate(H5I_GROUP, filter, data) < 0) {
			log_fatal("failed to iterate over file's open file IDs");
			_exit(EXIT_FAILURE);
		}
	}

	if (obj_types & H5F_OBJ_DATATYPE) {
		log_fatal("Does not support iteration over datatypes");
		_exit(EXIT_FAILURE);
	}

	if (obj_types & H5F_OBJ_ATTR) {
		log_fatal("Does not support iteration over attributes");
		_exit(EXIT_FAILURE);
	}
}

herr_t parh5F_get(void *obj, H5VL_file_get_args_t *fquery, hid_t dxpl_id, void **req)
{
	H5I_type_t *obj_type = (H5I_type_t *)obj;

	if (H5I_FILE != *obj_type) {
		log_fatal("Object should be a file");
		_exit(EXIT_FAILURE);
	}
	parh5F_file_t file = (parh5F_file_t)obj;
	switch (fquery->op_type) {
	case H5VL_FILE_GET_CONT_INFO:
		log_debug("H5VL_FILE_GET_CONT_INFO");
		break;
	case H5VL_FILE_GET_FAPL:
		log_debug("H5VL_FILE_GET_FAPL");
		break;
	case H5VL_FILE_GET_FCPL:;
		log_debug("H5VL_GET_FCPL");
		fquery->args.get_fcpl.fcpl_id = parh5F_get_fcpl(file);
		return PARH5_SUCCESS;
	case H5VL_FILE_GET_FILENO:
		log_debug("H5VL_FILE_GET_FILENO");
		break;
	case H5VL_FILE_GET_INTENT:
		log_debug("H5VL_FILE_GET_INTENT");
		break;
	case H5VL_FILE_GET_NAME:
		log_debug("H5VL_FILE_GET_NAME");

		if (NULL == fquery->args.get_name.buf) {
			*fquery->args.get_name.file_name_len = strlen(file->name) + 1;
			return PARH5_SUCCESS;
		}
		if (fquery->args.get_name.buf_size < strlen(file->name) + 1) {
			log_fatal("Overflow buffer too small buf size is %lu actual size: %lu",
				  fquery->args.get_name.buf_size, strlen(file->name) + 1);
			_exit(EXIT_FAILURE);
		}

		memcpy(fquery->args.get_name.buf, file->name, strlen(file->name) + 1);
		*fquery->args.get_name.file_name_len = strlen(file->name) + 1;
		return PARH5_SUCCESS;
	case H5VL_FILE_GET_OBJ_COUNT: {
		unsigned obj_types = fquery->args.get_obj_count.types;
		struct parh5F_obj_data udata = {
			.max_objs = 0, .oid_list = NULL, .obj_count = fquery->args.get_obj_count.count, .is_count = true
		};
		parh5F_iterate_objs(obj_types, parhh5F_get_obj_ids_callback, &udata);

		log_debug("FILE_GET_OBJ count returned %lu objects", *fquery->args.get_obj_count.count);
		return *udata.obj_count;
	}

	case H5VL_FILE_GET_OBJ_IDS: {
		unsigned obj_types = fquery->args.get_obj_ids.types;
		size_t max_ids = fquery->args.get_obj_ids.max_objs;

		if (max_ids == 0) {
			log_debug("Max ids are 0 nothong to do");
			return PARH5_SUCCESS; /*nothing to do*/
		}

		struct parh5F_obj_data udata = { .max_objs = fquery->args.get_obj_ids.max_objs,
						 .oid_list = fquery->args.get_obj_ids.oid_list,
						 .obj_count = fquery->args.get_obj_ids.count,
						 .is_count = false };

		parh5F_iterate_objs(obj_types, parhh5F_get_obj_ids_callback, &udata);
		return *udata.obj_count;
	}
	default:
		log_fatal("Unknown option");
		_exit(EXIT_FAILURE);
	}

	(void)dxpl_id;
	(void)req;
	log_fatal("Sorry unimplemented XXX TODO\n");
	_exit(EXIT_FAILURE);
	return PARH5_FAILURE;
}

static void parh5F_handle_file_flush(parh5F_file_t file, H5VL_file_specific_args_t *file_query)
{
	log_warn("Flush of file: %s not implemented yet XXX TODO XXX", file->name);
	switch (file_query->args.flush.obj_type) {
	case H5I_UNINIT:
	case H5I_BADID:
		log_debug("Bad type");
		_exit(EXIT_FAILURE);
	case H5I_FILE:
		break;
	default:
		_exit(EXIT_FAILURE);
	}
}

herr_t parh5F_specific(void *obj, H5VL_file_specific_args_t *file_query, hid_t dxpl_id, void **req)
{
	(void)dxpl_id;
	(void)req;
	H5I_type_t *obj_type = obj;
	if (obj && *obj_type != H5I_FILE) {
		log_fatal("Object is not a file!");
		_exit(EXIT_FAILURE);
	}
	parh5F_file_t file = obj;

	switch (file_query->op_type) {
	case H5VL_FILE_FLUSH:
		parh5F_handle_file_flush(file, file_query);
		log_debug("H5VL_FILE_FLUSH requested for file: %s XXX TODO XXX", file->name);
		return PARH5_SUCCESS;
	case H5VL_FILE_REOPEN:
		log_debug("H5VL_FILE_REOPEN requested propery list is:");
		break;
	case H5VL_FILE_IS_ACCESSIBLE:
		// const char *filename; /* Name of file to check */
		// hid_t fapl_id; /* File access property list to use */
		// hbool_t *accessible; /* Whether file is accessible with FAPL settings (OUT) */
		log_debug("H5VL_FILE_IS_ACCESSIBLE requested for file: %s XXX TODO XXX",
			  file_query->args.is_accessible.filename);
		parh5F_print_pl(file_query->args.is_accessible.fapl_id);
		*file_query->args.is_accessible.accessible = true;
		return PARH5_SUCCESS;
	case H5VL_FILE_DELETE:
		log_debug("H5VL_FILE_DELETE requested");
		break;
	case H5VL_FILE_IS_EQUAL:
		log_debug("H5VL_FILE_DELETE requested");
		break;
	default:
		log_debug("Unkown file operation type");
	}
	log_fatal("Sorry unimplemented XXX TODO\n");
	_exit(EXIT_FAILURE);
	return PARH5_FAILURE;
}

herr_t(parh5F_optional)(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_fatal("Sorry unimplemented XXX TODO\n");
	_exit(EXIT_FAILURE);
	return 1;
}

herr_t parh5F_close(void *file, hid_t dxpl_id, void **req)
{
	(void)dxpl_id;
	(void)req;
	H5I_type_t *type = file;
	if (H5I_FILE != *type) {
		log_fatal("Not a file to close");
		_exit(EXIT_FAILURE);
	}
	parh5F_file_t par_file = file;
	log_debug("Closing file: %s", par_file->name);
	parh5F_close_parallax_db(file);
	parh5G_group_t root_group = parh5F_get_root_group(file);
	parh5G_close(root_group, 0, NULL);
	log_debug("Closing file: %s....SUCCESS", par_file->name);
	free((char *)par_file->name);
	free(par_file);

	return PARH5_SUCCESS;
}
