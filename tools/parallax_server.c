#include "djb2.h"
#include "parh5_macros.h"
#include <H5Dpublic.h>
#include <H5Fpublic.h>
#include <H5Gpublic.h>
#include <H5Ppublic.h>
#include <H5Tpublic.h>
#include <assert.h>
#include <fcntl.h>
#include <log.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <uthash.h>
#define QUEUE_NAME "parallax_queue"

int parh5_server_fd;

static void parh5_send_command(struct parh5_cmd *cmd)
{
	const char *buf = (char *)cmd;
	size_t buf_size = sizeof(*cmd);
	size_t total_bytes_written = 0;
	while (total_bytes_written < buf_size) {
		// Write the commands to the FIFO queue
		ssize_t bytes_written =
			write(parh5_server_fd, &buf[total_bytes_written], buf_size - total_bytes_written);
		if (bytes_written == -1) {
			log_fatal("Failed to write create file command to FIFO queue");
			_exit(EXIT_FAILURE);
		}
		total_bytes_written += bytes_written;
	}
	assert(total_bytes_written == buf_size);
}

void parh5_server_file_create(hid_t parent_id, hid_t self_id, const char *file_name)
{
	struct parh5_cmd cmd = { 0 };
	cmd.cr_file.parent_loc_id = parent_id;
	cmd.cr_file.server_loc_id = self_id;
	strcpy(cmd.cr_file.file_name, file_name);
	parh5_send_command(&cmd);
}

void parh5_server_write_data(hid_t dataset_id, int ndims, hsize_t file_offset[], hsize_t file_count[],
			     hsize_t mem_offset[], hsize_t mem_count[], char *data, size_t data_size)
{
	struct parh5_cmd cmd = { .cmd = PARH5_WRITE_DATA };
	cmd.cr_write_data.dataset_id = dataset_id;
	for (int i = 0; i < ndims; i++) {
		cmd.cr_write_data.file_offset[i] = file_offset[i];
		cmd.cr_write_data.file_count[i] = file_count[i];
		cmd.cr_write_data.mem_offset[i] = mem_offset[i];
		cmd.cr_write_data.mem_count[i] = mem_count[i];
	}
	if (data_size > PARH5_MAX_BUFFER_SIZE) {
		log_fatal("Oops buffer too large");
		_exit(EXIT_FAILURE);
	}
	memcpy(cmd.cr_write_data.data, data, data_size);
	log_debug("Sending command of size: %lu data size: %lu", sizeof(cmd), data_size);
	parh5_send_command(&cmd);
}

void parh5_server_group_create(hid_t parent_id, hid_t self_id, const char *group_name)
{
	struct parh5_cmd cmd = { .cmd = PARH5_CREATE_GROUP };
	cmd.cr_group.parent_loc_id = parent_id;
	cmd.cr_group.server_loc_id = self_id;
	strcpy(cmd.cr_group.group_name, group_name);
	parh5_send_command(&cmd);
}

void parh5_server_dataset_create(hid_t parent_id, hid_t self_id, const char *dataset_name)
{
	struct parh5_cmd cmd = { .cmd = PARH5_CREATE_DATASET };
	cmd.cr_dataset.parent_loc_id = parent_id;
	cmd.cr_dataset.server_loc_id = self_id;
	hid_t datatype = H5Dget_type(self_id);
	if (datatype < 0) {
		log_fatal("Failed to get datatype");
		_exit(EXIT_FAILURE);
	}
	cmd.cr_dataset.datatype_class = H5Tget_class(datatype);
	hid_t dataspace_id = H5Dget_space(self_id);
	if (dataspace_id < 0) {
		log_fatal("Failed to create dataspace");
		_exit(EXIT_FAILURE);
	}
	int n_dims = H5Sget_simple_extent_ndims(dataspace_id);
	if (n_dims < 0) {
		log_fatal("Failed to get ndims");
		_exit(EXIT_FAILURE);
	}
	cmd.cr_dataset.n_dimesions = n_dims;
	H5Sget_simple_extent_dims(dataspace_id, cmd.cr_dataset.dimensions, NULL);
	strcpy(cmd.cr_dataset.dataset_name, dataset_name);
	parh5_send_command(&cmd);
}

int parh5_server_init(void)
{
	const char *fifo_path = PARH5_QUEUE_NAME;

	// Open the FIFO queue for writing
	parh5_server_fd = open(fifo_path, O_WRONLY);
	if (-1 == parh5_server_fd) {
		log_fatal("Failed to open FIFO queue for writing");
		_exit(EXIT_FAILURE);
	}
	struct parh5_cmd cmd = { .cmd = PARH5_TXN_START };
	// Write the commands to the FIFO queue
	ssize_t bytes_written = write(parh5_server_fd, &cmd, sizeof(cmd));
	if (bytes_written == -1) {
		log_fatal("Failed to write create file command to FIFO queue");
		_exit(EXIT_FAILURE);
	}
	return 1;
}

int parh5_server_close(void)
{
	log_debug("Terminating the transfer");
	struct parh5_cmd cmd = { .cmd = PARH5_TXN_END };
	// Write the commands to the FIFO queue
	ssize_t bytes_written = write(parh5_server_fd, &cmd, sizeof(cmd));
	if (bytes_written == -1) {
		log_fatal("Failed to write create file command to FIFO queue");
		_exit(EXIT_FAILURE);
	}

	return 1;
}
