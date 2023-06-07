#include "parallax_vol_inode.h"
#include "parallax/structures.h"
#include "parallax_vol_connector.h"
#include <log.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define PARH5I_NAME_SIZE 128
#define PARH5I_KEY_SIZE 9

struct parh5I_slot_entry {
	uint64_t inode_num;
	uint32_t offset; /*where the full name is actually*/
} __attribute((packed));

struct parh5I_inode {
	parh5_object_e type; /*group or dataset*/
	char name[PARH5I_NAME_SIZE];
	uint64_t inode_num;
	uint32_t size;
	uint32_t used;
	uint32_t n_items;
	char buffer[];
} __attribute((packed));

uint64_t parh5G_generate_inode_num(parh5I_inode_t root_inode, par_handle par_db)
{
	root_inode->inode_num++;
	parh5I_store_inode(root_inode, par_db);
	return root_inode->inode_num;
}

uint64_t parh5I_bsearch_inode(parh5I_inode_t inode, const char *pivot)
{
	if (inode->n_items == 0)
		return 0;
	struct parh5I_slot_entry *slot_array = (struct parh5I_slot_entry *)inode->buffer;
	uint32_t start = 0;
	uint32_t end = inode->n_items - 1;

	while (start <= end) {
		uint32_t middle = start + (end - start) / 2;

		int ret = strcmp(&inode->buffer[slot_array[middle].offset], pivot);

		if (ret == 0)
			return slot_array[middle].inode_num;

		if (ret > 0)
			start = middle + 1;
		else
			end = middle - 1;
	}
	return 0;
}

bool parh5I_add_inode(parh5I_inode_t inode, uint64_t inode_num, const char *name)
{
	uint32_t space_needed = strlen(name) + 1 + sizeof(struct parh5I_slot_entry);
	uint32_t offset = inode->size - (inode->used + space_needed);

	uint64_t rborder = (uint64_t)&inode->buffer[offset];
	uint64_t lborder = (uint64_t)&inode->buffer[(inode->n_items + 1) * sizeof(struct parh5I_slot_entry)];
	if (lborder > rborder)
		return false;

	memcpy(&inode->buffer[rborder], name, strlen(name) + 1);
	struct parh5I_slot_entry *slot_array = (struct parh5I_slot_entry *)inode->buffer;
	slot_array[inode->n_items].offset = offset;
	slot_array[inode->n_items].inode_num = inode_num;
	inode->used += space_needed;
	return true;
}

bool parh5I_store_inode(parh5I_inode_t inode, par_handle par_db)
{
	char key_buffer[PARH5I_KEY_SIZE] = { 'G' };
	memcpy(&key_buffer[1], &inode->inode_num, sizeof(inode->inode_num));
	struct par_key par_key = { .size = PARH5I_KEY_SIZE, .data = key_buffer };

	struct par_value new_par_value = { .val_size = sizeof(*inode) + inode->size, .val_buffer = (char *)inode };
	struct par_key_value KV = { .k = par_key, .v = new_par_value };
	const char *error = NULL;
	par_put(par_db, &KV, &error);

	if (error) {
		log_fatal("Failed to store inode of group %s", inode->name);
		_exit(EXIT_FAILURE);
	}
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
	return (parh5I_inode_t)par_value.val_buffer;
}

parh5I_inode_t parh5I_create_inode(const char *name, parh5_object_e type, parh5I_inode_t root_inode, par_handle par_db)
{
	parh5I_inode_t inode = calloc(1UL, sizeof(struct parh5I_inode) + PARH5I_INODE_SIZE);
	if (strlen(name) + 1 > PARH5I_NAME_SIZE) {
		log_fatal("Name: %s too large max size supported: %u got: %lu", name, PARH5I_NAME_SIZE,
			  strlen(name) + 1);
		_exit(EXIT_FAILURE);
	}
	memcpy(inode->name, name, strlen(name));
	inode->size = PARH5I_INODE_SIZE;
	inode->type = type;
	inode->inode_num = parh5G_generate_inode_num(root_inode, par_db);
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
	*size = inode->size;
	return inode ? inode->buffer : NULL;
}
