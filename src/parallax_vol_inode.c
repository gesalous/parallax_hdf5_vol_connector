#include "parallax_vol_inode.h"
#include "parallax/parallax.h"
#include "parallax/structures.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_dataset.h"
#include "parallax_vol_file.h"
#include "parallax_vol_group.h"
#include <H5Fpublic.h>
#include <H5Ipublic.h>
#include <H5VLconnector.h>
#include <H5VLconnector_passthru.h>
#include <assert.h>
#include <log.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define PARH5I_INODE_SIZE sizeof(struct parh5I_inode)
#define PARH5I_NAME_SIZE 128
#define PARH5I_GROUP_METADATA_BUFFER_SIZE 3096
#define PARH5I_KEY_SIZE 9
#define PARH5I_INODE_KEY_PREFIX 'I'
#define PARH5I_PIVOT_KEY_PREFIX 'P'
typedef struct parh5D_dataset *parh5D_dataset_t;

struct parh5I_pivot {
	uint64_t inode_num;
} __attribute((packed));

struct parh5I_inode {
	H5I_type_t type; /*group or dataset*/
	char name[PARH5I_NAME_SIZE];
	char metadata[PARH5I_GROUP_METADATA_BUFFER_SIZE]; /*acpl and cpl lists kept serialized here*/
	uint64_t inode_num;
	uint64_t counter;
	uint32_t pivot_size;
	uint32_t num_pivots;
} __attribute((packed));

static void parh5I_print_inode(parh5I_inode_t inode)
{
	log_debug("Inode name: %s", inode->name);
	log_debug("Inode number: %lu", inode->inode_num);
	log_debug("Inode type: %d", inode->type);
}

static bool parh5I_construct_pivot_key(const char *pivot_name, parh5I_inode_t inode, char *key_buffer,
				       size_t *key_buffer_size)
{
	static_assert(1 == 1UL, "pivot prefix must be one char");
	uint32_t needed_space = 1UL + sizeof(inode->inode_num) + strlen(pivot_name) + 1UL;
	if (needed_space > *key_buffer_size) {
		log_fatal("Key buffer too small needs %u B got :%lu B", needed_space, *key_buffer_size);
		_exit(EXIT_FAILURE);
	}
	int idx = 0;
	key_buffer[idx] = PARH5I_PIVOT_KEY_PREFIX;
	idx += 1UL;
	memcpy(&key_buffer[idx], &inode->inode_num, sizeof(inode->inode_num));
	idx += sizeof(inode->inode_num);
	memcpy(&key_buffer[idx], pivot_name, strlen(pivot_name) + 1);

	*key_buffer_size = needed_space;
	return true;
}

char *parh5I_get_inode_metadata_buf(parh5I_inode_t inode)
{
	return inode ? inode->metadata : NULL;
}

size_t parh5I_get_inode_metadata_size(void)
{
	return PARH5I_GROUP_METADATA_BUFFER_SIZE;
}

bool parh5I_is_root_inode(parh5I_inode_t inode)
{
	return inode ? inode->inode_num == 0 : false;
}

static bool parh5I_check_prefix(const char *prefix, size_t prefix_len, const char *key, size_t key_len)
{
	if (prefix_len > key_len)
		return false;
	return memcmp(prefix, key, prefix_len) == 0;
}

static void parh5I_add_dataset(parh5D_dataset_t dataset, H5VL_file_get_obj_ids_args_t *objs)
{
	hid_t dataset_id = H5VLwrap_register(dataset, H5I_DATASET);
	if (!H5Iis_valid(dataset_id)) {
		log_fatal("Failed to register dataset name: %s", parh5D_get_dataset_name(dataset));
		_exit(EXIT_FAILURE);
	}
	objs->oid_list[(*objs->count)++] = dataset_id;
	return;
}

static void parh5I_add_group(parh5G_group_t group, H5VL_file_get_obj_ids_args_t *objs)
{
	hid_t group_id = H5VLwrap_register(group, H5I_GROUP);
	if (!H5Iis_valid(group_id)) {
		log_fatal("Failed to register dataset name: %s", parh5G_get_group_name(group));
		_exit(EXIT_FAILURE);
	}
	objs->oid_list[(*objs->count)++] = group_id;

	return;
}

void parh5I_get_children_names(parh5I_inode_t inode, char **objs, size_t *obj_size, parh5F_file_t file)
{
	if (!inode) {
		log_fatal("NULL inode?");
		_exit(EXIT_FAILURE);
	}

	par_handle par_db = parh5F_get_parallax_db(file);
	// log_debug("Searching Inode: %s with inode num: %lu", inode->name, inode->inode_num);
	if (inode->type != H5I_GROUP && inode->type != H5I_DATASET) {
		log_fatal("Corrupted inode");
		_exit(EXIT_FAILURE);
	}

	size_t obj_id = 0;

	if (inode->type == H5I_DATASET) {
		log_warn("XXX TODO XXX you should check and return also the attributes if requested");
		return;
	}

	parh5G_group_t self_group = parh5G_open_group(file, inode);
	char start_key_buffer[1UL + sizeof(inode->inode_num)] = { PARH5I_PIVOT_KEY_PREFIX };
	memcpy(&start_key_buffer[1UL], &inode->inode_num, sizeof(inode->inode_num));
	const char *error = NULL;
	struct par_key start_key = { .size = sizeof(start_key_buffer), .data = start_key_buffer };
	par_scanner scanner = par_init_scanner(par_db, &start_key, PAR_GREATER, &error);
	if (error) {
		log_fatal("Failed to init scanner reason: %s", error);
		_exit(EXIT_FAILURE);
	}
	while (par_is_valid(scanner) && obj_id < *obj_size) {
		struct par_key pivot_key = par_get_key(scanner);
		struct par_value pivot_value = par_get_value(scanner);
		if (!parh5I_check_prefix(start_key_buffer, sizeof(start_key_buffer), pivot_key.data, pivot_key.size))
			break;
		struct parh5I_pivot *pivot = (struct parh5I_pivot *)pivot_value.val_buffer;
		parh5I_inode_t child_inode = parh5I_get_inode(par_db, pivot->inode_num);
		if (child_inode->type == H5I_GROUP)
			log_debug("Found group: %s", child_inode->name);
		else if (child_inode->type == H5I_DATASET)
			log_debug("Found dataset: %s", child_inode->name);
		else {
			log_fatal("Unsupported");
			_exit(EXIT_FAILURE);
		}
		if (obj_id >= *obj_size)
			break;
		objs[obj_id++] = strdup(child_inode->name);
		free(child_inode);
		par_get_next(scanner);
	}
	par_close_scanner(scanner);
	if (error) {
		log_fatal("Failed to close scanner");
		_exit(EXIT_FAILURE);
	}
	*obj_size = obj_id;
	free(self_group);
}

void parh5I_get_all_objects(parh5I_inode_t inode, H5VL_file_get_obj_ids_args_t *objs, parh5F_file_t file)
{
	if (!inode) {
		log_fatal("NULL inode?");
		_exit(EXIT_FAILURE);
	}

	static_assert(PARH5I_INODE_SIZE > PARH5I_NAME_SIZE + PARH5I_GROUP_METADATA_BUFFER_SIZE, "INODE size too small");
	par_handle par_db = parh5F_get_parallax_db(file);
	// log_debug("Searching Inode: %s with inode num: %lu", inode->name, inode->inode_num);
	if (inode->type != H5I_GROUP && inode->type != H5I_DATASET) {
		log_fatal("Corrupted inode");
		_exit(EXIT_FAILURE);
	}

	if (inode->type == H5I_DATASET) {
		parh5D_dataset_t dataset = parh5D_open_dataset(inode, file);

		hid_t dataset_id = H5VLwrap_register(dataset, H5I_DATASET);
		if (!H5Iis_valid(dataset_id)) {
			log_fatal("Failed to register dataset name: %s", inode->name);
			_exit(EXIT_FAILURE);
		}
		// log_debug("Added Dataset of name: %s at idx %lu", inode->name, *objs->count);
		objs->oid_list[(*objs->count)++] = dataset_id;
		return;
	}
	parh5G_group_t self_group = parh5G_open_group(file, inode);
	char start_key_buffer[1UL + sizeof(inode->inode_num)] = { PARH5I_PIVOT_KEY_PREFIX };
	memcpy(&start_key_buffer[1UL], &inode->inode_num, sizeof(inode->inode_num));
	const char *error = NULL;
	struct par_key start_key = { .size = sizeof(start_key_buffer), .data = start_key_buffer };
	par_scanner scanner = par_init_scanner(par_db, &start_key, PAR_GREATER, &error);
	if (error) {
		log_fatal("Failed to init scanner reason: %s", error);
		_exit(EXIT_FAILURE);
	}
	while (par_is_valid(scanner)) {
		struct par_key pivot_key = par_get_key(scanner);
		struct par_value pivot_value = par_get_value(scanner);
		if (!parh5I_check_prefix(start_key_buffer, sizeof(start_key_buffer), pivot_key.data, pivot_key.size))
			break;
		struct parh5I_pivot *pivot = (struct parh5I_pivot *)pivot_value.val_buffer;
		// log_debug("--->Got key: %.*s of size: %u from inode: %s inode_num is: %lu ", pivot_key.size,
		// 	  pivot_key.data, pivot_key.size, inode->name, pivot->inode_num);
		parh5I_inode_t child_inode = parh5I_get_inode(par_db, pivot->inode_num);
		// log_debug("--->Fetched child inode: %s", child_inode->name);
		parh5I_get_all_objects(child_inode, objs, file);
		par_get_next(scanner);
	}
	par_close_scanner(scanner);
	if (error) {
		log_fatal("Failed to close scanner");
		_exit(EXIT_FAILURE);
	}

	if (0 == strcmp(parh5G_get_group_name(self_group), "-ROOT-"))
		return;

	if (*objs->count >= objs->max_objs) {
		log_fatal("Overflow! objs_count is %ld max objs %ld", *objs->count, objs->max_objs);
		_exit(EXIT_FAILURE);
	}
	hid_t group_id = H5VLwrap_register(self_group, H5I_GROUP);
	if (!H5Iis_valid(group_id)) {
		log_fatal("Failed to register dataset name: %s", inode->name);
		_exit(EXIT_FAILURE);
	}
	objs->oid_list[(*objs->count)++] = group_id;
	log_debug("Added Group of name: %s at idx %lu", inode->name, *objs->count);
}

void *parh5I_find_object(parh5I_inode_t inode, const char *name, H5I_type_t *opened_type, parh5F_file_t file)
{
	/*XXX TODO XXX need to search all if name is an attribute*/

	if (NULL == file) {
		log_warn("NULL file?");
		return NULL;
	}

	if (!inode) {
		log_warn("NULL inode?");
		return NULL;
	}

	if (inode->type != H5I_GROUP && inode->type != H5I_DATASET) {
		log_fatal("Corrupted inode");
		_exit(EXIT_FAILURE);
	}

	par_handle parallax_db = parh5F_get_parallax_db(file);
	char *obj = NULL;

	if (inode->type == H5I_DATASET) {
		if (strcmp(name, inode->name))
			return NULL;
		parh5D_dataset_t dataset = parh5D_open_dataset(inode, file);
		log_debug("Found name: %s it is a dataset within file: %s", name, parh5F_get_file_name(file));
		*opened_type = H5I_DATASET;
		return dataset;
	}

	parh5G_group_t self_group = parh5G_open_group(file, inode);
	if (0 == strcmp(name, parh5G_get_group_name(self_group))) {
		*opened_type = H5I_GROUP;
		log_debug("Found name: %s it is a group within file: %s", name, parh5F_get_file_name(file));
		return self_group;
	}
	/*Continuing the search*/
	char start_key_buffer[1UL + sizeof(inode->inode_num)] = { PARH5I_PIVOT_KEY_PREFIX };
	memcpy(&start_key_buffer[1UL], &inode->inode_num, sizeof(inode->inode_num));
	const char *error = NULL;
	struct par_key start_key = { .size = sizeof(start_key_buffer), .data = start_key_buffer };
	par_scanner scanner = par_init_scanner(parallax_db, &start_key, PAR_GREATER, &error);
	if (error) {
		log_fatal("Failed to init scanner reason: %s", error);
		_exit(EXIT_FAILURE);
	}
	while (par_is_valid(scanner)) {
		struct par_key pivot_key = par_get_key(scanner);
		struct par_value pivot_value = par_get_value(scanner);
		if (!parh5I_check_prefix(start_key_buffer, sizeof(start_key_buffer), pivot_key.data, pivot_key.size))
			break;
		struct parh5I_pivot *pivot = (struct parh5I_pivot *)pivot_value.val_buffer;
		// log_debug("--->Got key: %.*s of size: %u from inode: %s inode_num is: %lu ", pivot_key.size,
		// 	  pivot_key.data, pivot_key.size, inode->name, pivot->inode_num);
		parh5I_inode_t child_inode = parh5I_get_inode(parallax_db, pivot->inode_num);
		// log_debug("--->Fetched child inode: %s", child_inode->name);
		obj = parh5I_find_object(child_inode, name, opened_type, file);
		if (NULL != obj)
			break;
		free(child_inode);
		par_get_next(scanner);
	}
	par_close_scanner(scanner);
	free(self_group);
	return obj;
}

uint64_t parh5I_get_obj_count(parh5I_inode_t root_inode)
{
	log_debug("obj_count in the system root_inode: %s is %lu", root_inode->name, root_inode->counter);
	assert(0);
	return root_inode ? root_inode->counter : 0;
}

uint64_t parh5I_generate_inode_num(parh5I_inode_t root_inode, par_handle par_db)
{
	if (!root_inode || !par_db) {
		log_debug("Root inode is NULL?");
		return 0;
	}
	root_inode->counter++;
	assert(0 == strcmp(root_inode->name, "-ROOT-"));
	log_debug("Storing root group!");
	parh5I_store_inode(root_inode, par_db);
	return root_inode->counter;
}

uint64_t parh5I_path_search(parh5I_inode_t inode, const char *path_search, par_handle par_db)
{
	if (NULL == inode)
		return 0;
	uint64_t inode_num = 0;
	parh5I_inode_t curr_inode = inode;

	char *str = strdup(path_search); // Create a copy of the input string
	char *token;
	char *delimiter = "/";

	token = strtok(str, delimiter);
	while (token != NULL) {
		inode_num = parh5I_lsearch_inode(curr_inode, token, par_db);
		// log_debug("Searching: %s at inode: %s got inode num: %lu\n", token, curr_inode->name, inode_num);
		if (inode != curr_inode)
			free(curr_inode);
		curr_inode = NULL;
		if (0 == inode_num)
			break; /*not found*/
		curr_inode = parh5I_get_inode(par_db, inode_num);
		token = strtok(NULL, delimiter);
	}
	if (curr_inode != inode)
		free(curr_inode);
	free(str);
	if (0 == inode_num)
		log_debug("Path :%s not found", path_search);
	return inode_num;
}

uint64_t parh5I_lsearch_inode(parh5I_inode_t inode, const char *pivot_name, par_handle par_db)
{
	if (NULL == inode)
		return 0;
	char key_buffer[1UL + sizeof(inode->inode_num) + PARH5I_NAME_SIZE];
	size_t key_buffer_size = sizeof(key_buffer);
	parh5I_construct_pivot_key(pivot_name, inode, key_buffer, &key_buffer_size);
	struct par_key par_key = { .size = key_buffer_size, .data = key_buffer };
	char value_buf[64] = { 0 };
	struct par_value par_value = { .val_buffer_size = sizeof(value_buf), .val_buffer = value_buf };
	const char *error = NULL;
	par_get(par_db, &par_key, &par_value, &error);
	if (error) {
		log_debug("Could not find key: %.*s of size: %lu pivot: %s reason: %s", par_key.size, par_key.data,
			  key_buffer_size, pivot_name, error);
		return 0;
	}
	struct parh5I_pivot *pivot = (struct parh5I_pivot *)value_buf;
	return pivot->inode_num;
}

bool parh5I_add_pivot_in_inode(parh5I_inode_t inode, uint64_t inode_num, const char *pivot_name, par_handle par_db)
{
	char key_buffer[1UL + sizeof(inode->inode_num) + PARH5I_NAME_SIZE];
	size_t key_buffer_size = sizeof(key_buffer);
	parh5I_construct_pivot_key(pivot_name, inode, key_buffer, &key_buffer_size);
	struct parh5I_pivot pivot = { .inode_num = inode_num };

	struct par_key par_key = { .size = key_buffer_size, .data = key_buffer };

	struct par_value new_par_value = { .val_size = sizeof(pivot),
					   .val_buffer_size = sizeof(pivot),
					   .val_buffer = (char *)&pivot };
	struct par_key_value KV = { .k = par_key, .v = new_par_value };
	const char *error = NULL;
	par_put(par_db, &KV, &error);

	if (error) {
		log_fatal("Failed to store inode of group %s", inode->name);
		_exit(EXIT_FAILURE);
	}
	inode->num_pivots++;
	parh5I_store_inode(inode, par_db);
	log_debug("Added pivot: %s key size is: %lu in inode: %s", pivot_name, key_buffer_size, inode->name);
	return true;
}

bool parh5I_store_inode(parh5I_inode_t inode, par_handle par_db)
{
	char key_buffer[PARH5I_KEY_SIZE] = { PARH5I_INODE_KEY_PREFIX };
	memcpy(&key_buffer[1], &inode->inode_num, sizeof(inode->inode_num));
	struct par_key par_key = { .size = sizeof(key_buffer), .data = key_buffer };

	struct par_value new_par_value = { .val_size = PARH5I_INODE_SIZE,
					   .val_buffer_size = PARH5I_INODE_SIZE,
					   .val_buffer = (char *)inode };
	struct par_key_value KV = { .k = par_key, .v = new_par_value };
	const char *error = NULL;
	par_put(par_db, &KV, &error);

	if (error) {
		log_fatal("Failed to store inode of group %s", inode->name);
		_exit(EXIT_FAILURE);
	}
	// log_debug("*******<STORED inode>");
	// parh5I_print_inode(inode);
	// log_debug("*******</STORED inode>");
	return true;
}

parh5I_inode_t parh5I_get_inode(par_handle par_db, uint64_t inode_num)
{
	char key_buffer[PARH5I_KEY_SIZE] = { PARH5I_INODE_KEY_PREFIX };
	memcpy(&key_buffer[1], &inode_num, sizeof(inode_num));
	struct par_key par_key = { .size = PARH5I_KEY_SIZE, .data = key_buffer };
	struct par_value par_value = { 0 };
	const char *error = NULL;
	par_get(par_db, &par_key, &par_value, &error);
	if (error) {
		log_debug("Could not find key: %.*s reason: %s", par_key.size, par_key.data, error);
		return NULL;
	}

	assert(par_value.val_size == PARH5I_INODE_SIZE);
	// parh5I_inode_t root_inode = (parh5I_inode_t)par_value.val_buffer;
	// log_debug("******<INODE Fetched: %s, num: %lu>", root_inode->name, root_inode->inode_num);
	// parh5I_print_inode(root_inode);
	// log_debug("******</INODE Fetched: %s>", root_inode->name);
	return (parh5I_inode_t)par_value.val_buffer;
}

parh5I_inode_t parh5I_create_inode(const char *name, H5I_type_t type, parh5I_inode_t root_inode, par_handle par_db)
{
	parh5I_inode_t inode = calloc(1UL, PARH5I_INODE_SIZE);
	if (strlen(name) + 1 > PARH5I_NAME_SIZE) {
		log_fatal("Name: %s too large max size supported: %u got: %lu", name, PARH5I_NAME_SIZE,
			  strlen(name) + 1);
		_exit(EXIT_FAILURE);
	}
	memcpy(inode->name, name, strlen(name));
	inode->pivot_size = 0;
	inode->type = type;

	assert(root_inode == NULL || 0 == strcmp(root_inode->name, "-ROOT-"));
	if (NULL == root_inode)
		inode->counter = 1;

	inode->inode_num = root_inode ? parh5I_generate_inode_num(root_inode, par_db) : 1;
	if (!inode->inode_num) {
		log_fatal("Failed to get a new inode num");
		_exit(EXIT_FAILURE);
	}
	return inode;
}

const char *parh5I_get_inode_name(parh5I_inode_t inode)
{
	return inode ? inode->name : NULL;
}

uint64_t parh5I_get_inode_num(parh5I_inode_t inode)
{
	return inode ? inode->inode_num : 0;
}

uint32_t parh5I_get_nlinks(parh5I_inode_t inode)
{
	return inode ? inode->num_pivots : 0;
}

void parh5I_increase_nlinks(parh5I_inode_t inode)
{
	inode->num_pivots++;
	if (inode->type == H5I_DATASET)
		log_debug("Added pivot/attribue now is: %d in dataset: %s", inode->num_pivots, inode->name);
	if (inode->type == H5I_GROUP)
		log_debug("Added pivot/attribue now is: %d in group: %s", inode->num_pivots, inode->name);
}
