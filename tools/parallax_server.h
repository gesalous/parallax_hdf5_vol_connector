#ifndef PARALLAX_SERVER_H
#define PARALLAX_SERVER_H
#include <H5Fpublic.h>
void parh5_server_file_create(hid_t parent_id, hid_t self_id, const char *file_name);

void parh5_server_group_create(hid_t parent_id, hid_t self_id, const char *group_name);

void parh5_server_dataset_create(hid_t parent_id, hid_t self_id, const char *dataset_name);
void parh5_server_write_data(hid_t dataset_id, int ndims, hsize_t file_offset[], hsize_t file_count[],
			     hsize_t mem_offset[], hsize_t mem_count[], char *data, size_t data_size);

int parh5_server_init(void);
int parh5_server_close(void);
#endif
