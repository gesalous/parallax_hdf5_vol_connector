#ifndef PARH5_MACROS_H
#include <H5Fpublic.h>
#include <H5Tpublic.h>
#define PARH5_MACROS_H
#define PARH5_QUEUE_NAME "parallax_queue"
#define PARH5_MAX_NAME_SIZE 256
#define PARH5_MAX_DIMENSIONS 256

// Define the command enum
typedef enum {
	PARH5_CREATE_FILE,
	PARH5_CREATE_GROUP,
	PARH5_CREATE_DATASET,
	PARH5_CREATE_ATTR,
	PARH5_WRITE_ATTR,
	PARH5_WRITE_DATA,
	PARH5_TXN_START,
	PARH5_TXN_END,
	PARH5_NUM_ELEMS
} parh5_command_e;

struct parh5_create_file {
	char file_name[PARH5_MAX_NAME_SIZE];
	hid_t parent_loc_id;
	hid_t server_loc_id;
};

struct parh5_create_group {
	char group_name[PARH5_MAX_NAME_SIZE];
	hid_t parent_loc_id;
	hid_t server_loc_id;
};

struct parh5_create_dataset {
	char group_name[PARH5_MAX_NAME_SIZE];
	hid_t parent_loc_id;
	hid_t server_loc_id;
	char dataset_name[PARH5_MAX_NAME_SIZE];
	hsize_t dimensions[PARH5_MAX_DIMENSIONS];
	H5T_class_t datatype_class;
	int n_dimesions;
};

struct parh5_cmd {
	parh5_command_e cmd;
	union {
		struct parh5_create_file cr_file;
		struct parh5_create_group cr_group;
		struct parh5_create_dataset cr_dataset;
	};
};
#endif
