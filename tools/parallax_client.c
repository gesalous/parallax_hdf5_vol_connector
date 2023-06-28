#include "djb2.h"
#include "parh5_macros.h"
#include <H5Fpublic.h>
#include <H5Gpublic.h>
#include <H5Ppublic.h>
#include <H5Tpublic.h>
#include <fcntl.h>
#include <log.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <uthash.h>
#define QUEUE_NAME "parallax_queue"

struct parh5_location {
	hid_t loc_id;
	hid_t server_loc_id;
	UT_hash_handle hh;
};

struct parh5_location *root_loc;
typedef void (*parh5_client_handler)(struct parh5_cmd *cmd, hid_t fapl_id);

static hid_t parh5_client_create_datatype(H5T_class_t class)
{
	switch (class) {
	case H5T_INTEGER:
		return H5Tcopy(H5T_NATIVE_INT);
	case H5T_FLOAT:
		return H5Tcopy(H5T_NATIVE_FLOAT);
	case H5T_TIME:
		return H5Tcopy(H5T_NATIVE_B8_g);
	case H5T_STRING:;
		hid_t datatype_id = H5Tcopy(H5T_C_S1);
		H5Tset_size(datatype_id, H5T_VARIABLE);
		return datatype_id;
	case H5T_BITFIELD:
	case H5T_OPAQUE:
	case H5T_COMPOUND:
	case H5T_REFERENCE:
	case H5T_ENUM:
	case H5T_VLEN:
	case H5T_ARRAY:
	case H5T_NCLASSES:
		log_fatal("Sorry currently unsupported");
		_exit(EXIT_FAILURE);
	default:
		log_fatal("Unknwon class");
		_exit(EXIT_FAILURE);
	}
}

static void parh5_client_file_create(struct parh5_cmd *cmd, hid_t fapl_id)
{
	struct parh5_create_file *cr_file = &cmd->cr_file;
	hid_t file_id = H5Fcreate(cr_file->file_name, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id);
	if (file_id < 0) {
		log_fatal("Failed to open parallax file: %s", cr_file->file_name);
		_exit(EXIT_FAILURE);
	}

	struct parh5_location *loc = calloc(1UL, sizeof(struct parh5_location));
	loc->loc_id = file_id;
	loc->server_loc_id = cr_file->server_loc_id;
	HASH_ADD_PTR(root_loc, server_loc_id, loc);
	log_debug("Created parallax file: %s added loc mapping: %ld", cr_file->file_name, loc->server_loc_id);
}

static void parh5_client_group_create(struct parh5_cmd *cmd, hid_t gapl_id)
{
	struct parh5_create_group *cr_group = &cmd->cr_group;
	struct parh5_location *loc = NULL;
	HASH_FIND_PTR(root_loc, &cr_group->parent_loc_id, loc);
	if (NULL == loc) {
		log_fatal("Failed to find a mapping for location hid_t: %ld for file name: %s during group creation",
			  cr_group->parent_loc_id, cr_group->group_name);
		_exit(EXIT_FAILURE);
	}

	hid_t gid = H5Gcreate2(loc->loc_id, cr_group->group_name, H5P_DEFAULT, H5P_DEFAULT, gapl_id);
	if (gid < 0) {
		log_fatal("Failed to create group: %s in parallax", cr_group->group_name);
		_exit(EXIT_FAILURE);
	}
	struct parh5_location *new_loc = calloc(1UL, sizeof(struct parh5_location));
	new_loc->loc_id = gid;
	new_loc->server_loc_id = cr_group->server_loc_id;
	HASH_ADD_PTR(root_loc, server_loc_id, new_loc);
	log_debug("Group: %s Added mapping between local group: %ld remote group: %ld (key)", cr_group->group_name,
		  new_loc->loc_id, new_loc->server_loc_id);
}

static void parh5_client_dataset_create(struct parh5_cmd *cmd, hid_t dapl_id)
{
	struct parh5_create_dataset *cr_dataset = &cmd->cr_dataset;
	struct parh5_location *loc = NULL;
	HASH_FIND_PTR(root_loc, &cr_dataset->parent_loc_id, loc);
	if (NULL == loc) {
		log_fatal("Failed to find an hid_t for parent: %ld during dataset creation", cr_dataset->parent_loc_id);
		_exit(EXIT_FAILURE);
	}
	// Create dataspace for the dataset
	log_debug("n_dimensions  = %d", cr_dataset->n_dimesions);
	for (int i = 0; i < cr_dataset->n_dimesions; i++)
		printf("dim[%d] = %ld", i, cr_dataset->dimensions[i]);
	printf("\n");

	hid_t dataspace_id = H5Screate_simple(cr_dataset->n_dimesions, cr_dataset->dimensions, NULL);
	if (dataspace_id < 0) {
		log_fatal("Failed to create dataspace for dataset: %s", cr_dataset->dataset_name);
	}

	hid_t datatype_id = parh5_client_create_datatype(cr_dataset->datatype_class);
	if (datatype_id < 0) {
		log_fatal("Failed to create datatype");
		_exit(EXIT_FAILURE);
	}

	hid_t dataset_id = H5Dcreate2(loc->loc_id, cr_dataset->dataset_name, datatype_id, dataspace_id, H5P_DEFAULT,
				      H5P_DEFAULT, dapl_id);
	if (dataset_id < 0) {
		log_fatal("Failed to create dataset: %s in parallax", cr_dataset->dataset_name);
		_exit(EXIT_FAILURE);
	}
	struct parh5_location *new_loc = calloc(1UL, sizeof(struct parh5_location));
	new_loc->loc_id = dataset_id;
	new_loc->server_loc_id = cr_dataset->server_loc_id;
	HASH_ADD_PTR(root_loc, server_loc_id, new_loc);
	log_debug("Dataset: %s Added mapping between local dataset: %ld remote dataset: %ld (key)",
		  cr_dataset->dataset_name, new_loc->loc_id, new_loc->server_loc_id);
}

int main(void)
{
	parh5_client_handler dispatch_table[PARH5_NUM_ELEMS] = { parh5_client_file_create, parh5_client_group_create,
								 parh5_client_dataset_create };
	const char *fifo_path = PARH5_QUEUE_NAME;

	// Create the FIFO queue if it doesn't exist
	if (mkfifo(fifo_path, 0666) < 0) {
		log_warn("Failed to create FIFO queue for reading");
		perror("Reason:");
	}

	// Open the FIFO queue for reading
	int fd = open(fifo_path, O_RDONLY);
	if (fd < 0) {
		log_warn("Failed to open FIFO queue for reading");
		perror("Reason:");
		_exit(EXIT_FAILURE);
	}
	log_debug("Ready to serve commands");
	bool txn_start = false;
	// Read commands from the FIFO queue
	while (1) {
		struct parh5_cmd cmd = { 0 };

		// Read the command from the FIFO queue
		ssize_t bytes_read = read(fd, &cmd, sizeof(cmd));
		if (bytes_read == -1) {
			log_fatal("Failed to read command from FIFO queue");
			_exit(EXIT_FAILURE);
		} else if (bytes_read == 0)
			// End of file reached, FIFO queue closed
			break;
		log_debug("CMD read full? %s type: %d", bytes_read == sizeof(cmd) ? "YES" : "NO", cmd.cmd);
		// Process the command
		switch (cmd.cmd) {
		case PARH5_TXN_START:
			txn_start = true;
			continue;
		case PARH5_TXN_END:
			goto close;
		case PARH5_CREATE_FILE:
		case PARH5_CREATE_GROUP:
		case PARH5_CREATE_DATASET:
		case PARH5_CREATE_ATTR:
		case PARH5_WRITE_ATTR:
		case PARH5_WRITE_DATA:
			break;
		default:
			log_fatal("Unknown command");
			_exit(EXIT_FAILURE);
		}
		if (!txn_start) {
			log_debug("Stale commands ommiting");
			continue;
		}
		dispatch_table[cmd.cmd](&cmd, H5P_DEFAULT);
	}
close:
	// Close the FIFO queue
	if (close(fd) < 0) {
		log_fatal("Failed to close the fifo queue");
		perror("Reason:");
		_exit(EXIT_FAILURE);
	}

	return 0;
}