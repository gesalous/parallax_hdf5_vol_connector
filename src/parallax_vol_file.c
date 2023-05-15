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

struct parh5F_file {
	const char *file_name;
	par_handle db;
};

static inline const char *parh5F_get_file_name(parh5F_file_t file)
{
	return file ? file->file_name : NULL;
}

static inline par_handle parh5F_get_parallax_db(parh5F_file_t file)
{
	return file ? file->db : NULL;
}

static parh5F_file_t parh5F_new_file(const char *file_name)
{
	par_db_options db_options = { .volume_name = PARALLAX_VOLUME,
				      .create_flag = PAR_CREATE_DB,
				      .db_name = file_name,
				      .options = par_get_default_options() };
	db_options.options[LEVEL0_SIZE].value = PARALLAX_L0_SIZE;
	db_options.options[GROWTH_FACTOR].value = PARALLAX_GROWTH_FACTOR;
	db_options.options[PRIMARY_MODE].value = 1;

	parh5F_file_t file = calloc(1, sizeof(struct parh5F_file));
	const char *error_message = NULL;
	file->db = par_open(&db_options, &error_message);

	if (error_message) {
		log_fatal("Error uppon opening the DB, error %s", error_message);
		_exit(EXIT_FAILURE);
	}
	file->file_name = strdup(file_name);
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
	(void)name;
	(void)flags;
	(void)fcpl_id;
	(void)fapl_id;
	(void)dxpl_id;
	(void)req;
	log_debug("PAR_FILE_class name: %s flags %s", name, parh5F_flags2s(flags));
	return parh5F_new_file(name);
}

void *parh5F_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
	(void)name;
	(void)flags;
	(void)fapl_id;
	(void)dxpl_id;
	(void)req;

	log_debug("PAR_FILE_class name is %s", name);
	log_debug("PAR_FILE_class name: %s flags %s", name, parh5F_flags2s(flags));
	return parh5F_new_file(name);
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

herr_t parh5F_specific(void *obj, H5VL_file_specific_args_t *args, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_fatal("Sorry unimplemented XXX TODO\n");
	_exit(EXIT_FAILURE);
	return 1;
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
	log_debug("Closing file: %s", par_file->file_name);
	if (!par_file) {
		log_fatal("NULL file to close?");
		_exit(EXIT_FAILURE);
	}
	parh5F_close_parallax_db((parh5F_file_t)file);

	return 1;
}
