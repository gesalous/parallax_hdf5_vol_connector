#include "parallax_vol_inode.h"
#include "parallax/structures.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_dataset.h"
#include "parallax_vol_file.h"
#include "parallax_vol_group.h"
#include <H5VLconnector.h>
#include <assert.h>
#include <log.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define PARH5I_NAME_SIZE 128
#define PARH5I_KEY_SIZE 9

typedef struct parh5D_dataset *parh5D_dataset_t;

struct parh5I_slot_entry {
	uint64_t inode_num;
	uint32_t offset; /*where the full name is actually*/
} __attribute((packed));

struct parh5I_inode {
	parh5_object_e type; /*group or dataset*/
	char name[PARH5I_NAME_SIZE];
	uint64_t inode_num;
	uint64_t counter;
	uint32_t size;
	uint32_t used;
	uint32_t n_items;
	struct parh5I_slot_entry slot_array[];
} __attribute((packed));

// static void parh5I_print_inode(parh5I_inode_t inode)
// {
// 	log_debug("Inode name: %s", inode->name);
// 	log_debug("Inode size: %u", inode->size);
// 	log_debug("Inode used: %u", inode->used);
// 	log_debug("Inode number: %lu", inode->inode_num);
// 	log_debug("Inode nitems: %u", inode->n_items);
// 	log_debug("Inode type: %d", inode->type);
// }

void parh5I_get_all_objects(parh5I_inode_t inode, H5VL_file_get_obj_ids_args_t *objs, parh5F_file_t file)
{
	par_handle par_db = parh5F_get_parallax_db(file);
	log_debug("Inode: %s", inode->name);
	if (inode->type != PARH5_GROUP && inode->type != PARH5_DATASET) {
		log_fatal("Corrupted inode");
		_exit(EXIT_FAILURE);
	}

	if (inode->type == PARH5_DATASET) {
		parh5D_dataset_t dataset = parh5D_open_dataset(inode, file);
		log_debug("Added Dataset of name: %s at idx %lu", inode->name, *objs->count);
		objs->oid_list[(*objs->count)++] = (hid_t)dataset;
		return;
	}
	parh5G_group_t self_group = parh5G_open_group(file, inode);
	// Recursive call for directories
	for (uint32_t i = 0; i < inode->n_items; i++) {
		parh5I_inode_t child_inode = parh5I_get_inode(par_db, inode->slot_array[i].inode_num);
		parh5I_get_all_objects(child_inode, objs, file);
	}

	if (*objs->count >= objs->max_objs) {
		log_fatal("Overflow! objs_count is %ld max objs %ld", *objs->count, objs->max_objs);
		_exit(EXIT_FAILURE);
	}
	log_debug("Adding Group of name: %s at idx %lu", inode->name, *objs->count);
	objs->oid_list[(*objs->count)++] = (hid_t)self_group;
}

uint64_t parh5I_get_obj_count(parh5I_inode_t root_inode)
{
	log_debug("obj_count in the system root_inode: %s is %lu", root_inode->name, root_inode->counter);
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
		inode_num = parh5I_lsearch_inode(curr_inode, token);
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

uint64_t parh5I_lsearch_inode(parh5I_inode_t inode, const char *pivot)
{
	if (NULL == inode)
		return 0;

	struct parh5I_slot_entry *slot_array = inode->slot_array;
	char *buffer = (char *)inode;
	for (uint32_t i = 0; i < inode->n_items; i++) {
		char *index_string = &buffer[slot_array[i].offset];
		// log_debug("offset to examine: %u string %s:%lu with pivot %s:%lu", slot_array[i].offset, index_string,
		// 	  strlen(index_string), pivot, strlen(pivot));
		if (0 == strcmp(index_string, pivot))
			return slot_array[i].inode_num;
	}
	return 0;
}

bool parh5I_add_inode(parh5I_inode_t inode, uint64_t inode_num, const char *pivot)
{
	char *buffer = (char *)inode;
	uint32_t pivot_size = strlen(pivot) + 1;
	uint32_t roffset = inode->size - (inode->used + pivot_size);
	// log_debug("space_needed: %u offset to append is: %u inode size: %u nitems: %u", space_needed, offset,
	// 	  inode->size, inode->n_items);

	uint32_t loffset = sizeof(struct parh5I_inode) + ((inode->n_items + 1) * sizeof(struct parh5I_slot_entry));
	log_debug("loffset: %u roffset: %u", loffset, roffset);
	if ((uint64_t)&buffer[loffset] > (uint64_t)&buffer[roffset])
		return false;

	memcpy(&buffer[roffset], pivot, pivot_size);
	inode->slot_array[inode->n_items].offset = roffset;
	inode->slot_array[inode->n_items].inode_num = inode_num;
	inode->used += pivot_size;
	inode->n_items++;
	log_debug("Added inode name:%s number: %lu", pivot, inode_num);

	return true;
}

bool parh5I_store_inode(parh5I_inode_t inode, par_handle par_db)
{
	char key_buffer[PARH5I_KEY_SIZE] = { 'G' };
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
	char key_buffer[PARH5I_KEY_SIZE] = { 'G' };
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
	// log_debug("******<INODE Fetched: %s, num: %lu, num_entries:%u>", root_inode->name, root_inode->inode_num,
	// 	  root_inode->n_items);
	// parh5I_print_inode(root_inode);
	// log_debug("******</INODE Fetched: %s with %u entries>", root_inode->name, root_inode->n_items);
	return (parh5I_inode_t)par_value.val_buffer;
}

parh5I_inode_t parh5I_create_inode(const char *name, parh5_object_e type, parh5I_inode_t root_inode, par_handle par_db)
{
	parh5I_inode_t inode = calloc(1UL, PARH5I_INODE_SIZE);
	if (strlen(name) + 1 > PARH5I_NAME_SIZE) {
		log_fatal("Name: %s too large max size supported: %u got: %lu", name, PARH5I_NAME_SIZE,
			  strlen(name) + 1);
		_exit(EXIT_FAILURE);
	}
	memcpy(inode->name, name, strlen(name));
	inode->size = PARH5I_INODE_SIZE;
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

char *parh5I_get_inode_buf(parh5I_inode_t inode, uint32_t *size)
{
	if (!inode)
		return NULL;

	if (inode->type != PARH5_DATASET) {
		log_fatal("Operation permittted only for group inodes");
		_exit(EXIT_FAILURE);
	}

	*size = PARH5I_INODE_SIZE - sizeof(struct parh5I_inode);
	return (char *)inode->slot_array;
}
