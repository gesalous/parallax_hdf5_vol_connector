#include "parallax_vol_file.h"
#include "H5VLconnector.h"
#include "H5public.h"
#include "parallax/structures.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_group.h"
#include "parallax_vol_inode.h"
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
typedef struct parh5G_group *parh5G_group_t;

struct parh5F_file {
	parh5_object_e obj_type;
	const char *name;
	parh5G_group_t root_group;
	par_handle db;
};

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

static parh5F_file_t parh5F_new_file(const char *file_name, enum par_db_initializers open_flag)
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
	file->db = par_open(&db_options, &error_message);
	if (error_message)
		log_debug("Parallax says: %s", error_message);

	if (file->db == NULL && error_message) {
		log_fatal("Error uppon opening the DB, error %s", error_message);
		_exit(EXIT_FAILURE);
	}
	file->name = strdup(file_name);
	file->obj_type = PARH5_FILE;

	/*Check it the root inode exists*/
	parh5I_inode_t root_inode = parh5I_get_inode(file->db, 1);
	if (root_inode) {
		file->root_group = parh5G_open_group(file, root_inode);
		log_debug("Opened root group for file: %s", file_name);
		return file;
	}

	file->root_group = parh5G_create_group(file, "-ROOT-");
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
	return parh5F_new_file(name, PAR_CREATE_DB);
}

void *parh5F_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
	(void)name;
	(void)flags;
	(void)fapl_id;
	(void)dxpl_id;
	(void)req;

	log_debug("Opening file name: %s flags %s", name, parh5F_flags2s(flags));
	return parh5F_new_file(name, PAR_CREATE_DB);
}

herr_t parh5F_get(void *obj, H5VL_file_get_args_t *args, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_fatal("Sorry unimplemented XXX TODO\n");
	_exit(EXIT_FAILURE);
	return 1;
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
	parh5_object_e *obj_type = obj;
	if (obj && *obj_type != PARH5_FILE) {
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
	(void)file;
	(void)dxpl_id;
	(void)req;
	parh5F_file_t par_file = file;
	log_debug("Closing file: %s", par_file->name);
	if (!par_file) {
		log_fatal("NULL file to close?");
		_exit(EXIT_FAILURE);
	}
	parh5F_close_parallax_db((parh5F_file_t)file);

	return 1;
}
