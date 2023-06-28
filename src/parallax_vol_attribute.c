#include "parallax_vol_attribute.h"
#include "djb2.h"
#include "parallax_vol_connector.h"
#include "parallax_vol_dataset.h"
#include "parallax_vol_file.h"
#include "parallax_vol_group.h"
#include "parallax_vol_inode.h"
#include <H5Ipublic.h>
#include <H5Spublic.h>
#include <H5Tpublic.h>
#include <H5VLconnector.h>
#include <assert.h>
#include <locale.h>
#include <log.h>
#include <parallax/parallax.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define PARH5A_ATTR_KEY_PREFIX 'A'
#define PARH5A_MAX_ATTR_NAME 64

struct parh5A_object {
	H5I_type_t type;
	union {
		parh5G_group_t group;
		parh5D_dataset_t dataset;
	};
};

struct parh5A_attribute {
	H5I_type_t type;
	struct parh5A_object parent;
	char name[PARH5A_MAX_ATTR_NAME];
	hid_t type_id;
	hid_t space_id;
	uint32_t value_size;
	char *value;
};

static void parh5A_print_buffer_Hex(const unsigned char *buffer, size_t size)
{
	for (size_t i = 0; i < size; i++) {
		fprintf(stderr, "%02X ", buffer[i]);
	}
	fprintf(stderr, "\n");
}

static bool parh5A_serialize_attr(parh5A_attribute_t attr, char *buffer, size_t *size)
{
	size_t type_size = UINT64_MAX;
	H5Tencode(attr->type_id, NULL, &type_size);

	size_t space_size = UINT64_MAX;
	H5Sencode2(attr->space_id, NULL, &space_size, 0);
	// log_debug("Serializing attr: %s Type size needed :%lu space_size needed: %lu buffer size: %lu", attr->name,
	// 	  type_size, space_size, *size);

	size_t space_needed = sizeof(attr->type) + PARH5A_MAX_ATTR_NAME + type_size + space_size +
			      sizeof(attr->value_size) + attr->value_size;

	if (space_needed > *size) {
		log_debug("Serializing attr: %s Buffer too small needs %lu actual %lu retry", attr->name, space_needed,
			  *size);
		*size = space_needed;
		return false;
	}
	uint32_t idx = 0;
	memcpy(&buffer[idx], &attr->type, sizeof(attr->type));
	idx += sizeof(attr->type);

	memcpy(&buffer[idx], attr->name, strlen(attr->name) + 1);
	idx += PARH5A_MAX_ATTR_NAME;

	if (H5Tencode(attr->type_id, &buffer[idx], &type_size) < 0) {
		log_fatal("Failed to serialize type");
		_exit(EXIT_FAILURE);
	}
	idx += type_size;

	if (H5Sencode2(attr->space_id, &buffer[idx], &space_size, 0) < 0) {
		log_fatal("Failed to serialize space");
		_exit(EXIT_FAILURE);
	}

	idx += space_size;
	memcpy(&buffer[idx], &attr->value_size, sizeof(attr->value_size));
	idx += sizeof(attr->value_size);
	memcpy(&buffer[idx], attr->value, attr->value_size);
	return true;
}

static bool parh5A_store_attr(parh5A_attribute_t attr)
{
	par_handle par_db = NULL;

	parh5I_inode_t inode = NULL;
	parh5F_file_t file = NULL;
	if (H5I_DATASET == attr->parent.type) {
		log_debug("We have a dataset");
		inode = parh5D_get_inode(attr->parent.dataset);
		file = parh5D_get_file(attr->parent.dataset);
	} else if (H5I_GROUP == attr->parent.type) {
		log_debug("We have a group");
		inode = parh5G_get_inode(attr->parent.group);
		file = parh5G_get_file(attr->parent.group);
	} else {
		log_fatal("Cannot handle this type of object");
		_exit(EXIT_FAILURE);
	}
	assert(inode);
	uint64_t parent_inode_num = parh5I_get_inode_num(inode);
	par_db = parh5F_get_parallax_db(file);

	size_t size = 0;
	parh5A_serialize_attr(attr, NULL, &size);
	char *buffer = calloc(1UL, size);

	if (!parh5A_serialize_attr(attr, buffer, &size)) {
		log_fatal("Failed to serialize attribute: %s", attr->name);
		_exit(EXIT_FAILURE);
	}
	uint64_t hash = djb2_hash((const unsigned char *)attr->name, strlen(attr->name) + 1);
	char key_buffer[1UL + sizeof(parent_inode_num) + sizeof(hash)] = { PARH5A_ATTR_KEY_PREFIX };
	memcpy(&key_buffer[1UL], &parent_inode_num, sizeof(parent_inode_num));
	memcpy(&key_buffer[1UL + sizeof(parent_inode_num)], &hash, sizeof(hash));

	struct par_key par_key = { .size = sizeof(key_buffer), .data = key_buffer };

	struct par_value new_par_value = { .val_size = size, .val_buffer_size = size, .val_buffer = buffer };
	struct par_key_value KV = { .k = par_key, .v = new_par_value };
	const char *error = NULL;
	par_put(par_db, &KV, &error);

	if (error) {
		log_fatal("Failed to store inode attribute name: %s for parent inode: %lu", attr->name,
			  parent_inode_num);
		_exit(EXIT_FAILURE);
	}
	setlocale(LC_ALL, "");
	log_debug("Storing attr name: %s size: %u belonging to: %s contents: %ls", attr->name, attr->value_size,
		  attr->parent.type == H5I_GROUP ? parh5G_get_group_name(attr->parent.group) :
						   parh5D_get_dataset_name(attr->parent.dataset),
		  (wchar_t *)attr->value);

	// parh5A_print_buffer_Hex((const unsigned char *)attr->value, attr->value_size);
	// log_debug("Attr key in parallax is:");
	// parh5A_print_buffer_Hex((const unsigned char *)key_buffer, sizeof(key_buffer));

	free(buffer);

	return true;
}

static bool parh5A_delete_attr(struct parh5A_object *obj, const char *attr_name)
{
	par_handle par_db = NULL;

	parh5I_inode_t inode = NULL;
	parh5F_file_t file = NULL;
	if (H5I_DATASET == obj->type) {
		parh5D_dataset_t dataset = obj->dataset;
		inode = parh5D_get_inode(dataset);
		assert(inode);
		file = parh5D_get_file(dataset);
	} else if (H5I_GROUP == obj->type) {
		parh5G_group_t group = obj->group;
		inode = parh5G_get_inode(group);
		assert(inode);
		file = parh5G_get_file(group);
	} else {
		log_fatal("Cannot handle this type of object");
		_exit(EXIT_FAILURE);
	}
	uint64_t parent_inode_num = parh5I_get_inode_num(inode);
	par_db = parh5F_get_parallax_db(file);
	uint64_t hash = djb2_hash((const unsigned char *)attr_name, strlen(attr_name) + 1);
	char key_buffer[1UL + sizeof(parent_inode_num) + sizeof(hash)] = { PARH5A_ATTR_KEY_PREFIX };
	memcpy(&key_buffer[1UL], &parent_inode_num, sizeof(parent_inode_num));
	memcpy(&key_buffer[1UL + sizeof(parent_inode_num)], &hash, sizeof(hash));

	struct par_key par_key = { .size = sizeof(key_buffer), .data = key_buffer };
	const char *error_message = NULL;
	par_delete(par_db, &par_key, &error_message);
	if (error_message) {
		log_debug("Delete of attr name: %s failed reason %s", attr_name, error_message);
		return false;
	}
	log_debug("Deleted attr name: %s successfully", attr_name);
	return true;
}

static bool parh5A_exists_attr(const char *attr_name, struct parh5A_object *object)
{
	if (!object)
		return false;

	par_handle par_db = NULL;

	parh5I_inode_t inode = NULL;
	parh5F_file_t file = NULL;
	if (H5I_DATASET == object->type) {
		parh5D_dataset_t dataset = object->dataset;
		inode = parh5D_get_inode(dataset);
		assert(inode);
		file = parh5D_get_file(dataset);
	} else if (H5I_GROUP == object->type) {
		parh5G_group_t group = object->group;
		inode = parh5G_get_inode(group);
		assert(inode);
		file = parh5G_get_file(group);
	} else {
		log_fatal("Cannot handle this type of object");
		_exit(EXIT_FAILURE);
	}
	uint64_t parent_inode_num = parh5I_get_inode_num(inode);
	par_db = parh5F_get_parallax_db(file);

	uint64_t hash = djb2_hash((const unsigned char *)attr_name, strlen(attr_name) + 1);
	char key_buffer[1UL + sizeof(parent_inode_num) + sizeof(hash)] = { PARH5A_ATTR_KEY_PREFIX };
	memcpy(&key_buffer[1UL], &parent_inode_num, sizeof(parent_inode_num));
	memcpy(&key_buffer[1UL + sizeof(parent_inode_num)], &hash, sizeof(hash));

	struct par_key par_key = { .size = sizeof(key_buffer), .data = key_buffer };
	return par_exists(par_db, &par_key) == PAR_SUCCESS;
}

static parh5A_attribute_t parh5A_get_attr(const char *attr_name, struct parh5A_object *object)
{
	if (!object)
		return false;

	par_handle par_db = NULL;

	parh5I_inode_t inode = NULL;
	parh5F_file_t file = NULL;
	if (H5I_DATASET == object->type) {
		parh5D_dataset_t dataset = object->dataset;
		inode = parh5D_get_inode(dataset);
		file = parh5D_get_file(dataset);
	} else if (H5I_GROUP == object->type) {
		parh5G_group_t group = object->group;
		inode = parh5G_get_inode(group);
		file = parh5G_get_file(group);
	} else {
		log_fatal("Cannot handle this type of object");
		_exit(EXIT_FAILURE);
	}
	uint64_t parent_inode_num = parh5I_get_inode_num(inode);
	par_db = parh5F_get_parallax_db(file);

	uint64_t hash = djb2_hash((const unsigned char *)attr_name, strlen(attr_name) + 1);
	char key_buffer[1UL + sizeof(parent_inode_num) + sizeof(hash)] = { PARH5A_ATTR_KEY_PREFIX };
	memcpy(&key_buffer[1UL], &parent_inode_num, sizeof(parent_inode_num));
	memcpy(&key_buffer[1UL + sizeof(parent_inode_num)], &hash, sizeof(hash));

	struct par_key par_key = { .size = sizeof(key_buffer), .data = key_buffer };
	struct par_value par_value = { 0 };
	const char *error = NULL;
	par_get(par_db, &par_key, &par_value, &error);
	if (error) {
		log_debug("Could not find key: %.*s reason: %s", par_key.size, par_key.data, error);
		return NULL;
	}
	parh5A_attribute_t attribute = calloc(1UL, sizeof(struct parh5A_attribute));
	size_t idx = 0;
	memcpy(&attribute->type, &par_value.val_buffer[idx], sizeof(attribute->type));
	idx += sizeof(attribute->type);
	memcpy(attribute->name, &par_value.val_buffer[idx], PARH5A_MAX_ATTR_NAME);
	idx += PARH5A_MAX_ATTR_NAME;
	attribute->type_id = H5Tdecode(&par_value.val_buffer[idx]);
	size_t size = 0;
	if (H5Tencode(attribute->type_id, NULL, &size) < 0) {
		log_fatal("Failed to calculate size");
		_exit(EXIT_FAILURE);
	}
	idx += size;
	attribute->space_id = H5Sdecode(&par_value.val_buffer[idx]);
	if (H5Sencode2(attribute->space_id, NULL, &size, 0) < 0) {
		log_fatal("Failed to calculate size");
		_exit(EXIT_FAILURE);
	}
	idx += size;
	memcpy(&attribute->value_size, &par_value.val_buffer[idx], sizeof(attribute->value_size));
	idx += sizeof(attribute->value_size);
	attribute->value = calloc(1UL, attribute->value_size + 1);
	memcpy(attribute->value, &par_value.val_buffer[idx], attribute->value_size);
	free(par_value.val_buffer);
	log_debug("Attribute: %s value size: %u contents: %.*s", attr_name, attribute->value_size,
		  attribute->value_size, attribute->value);
	log_debug("Attr key in parallax");
	parh5A_print_buffer_Hex((const unsigned char *)key_buffer, sizeof(key_buffer));
	return attribute;
}

static const char *parh5A_HPItype_to_string(H5I_type_t type)
{
	if (H5I_UNINIT == type)
		return "H5I_UNINIT";
	if (H5I_BADID == type)
		return "H5I_BADID";
	char *types[H5I_NTYPES] = { "Error",	       "H5I_FILE",	"H5I_GROUP",	   "H5I_DATATYPE",
				    "H5I_DATASPACE",   "H5I_DATASET",	"H5I_MAP",	   "H5I_ATTR",
				    "H5I_VFL",	       "H5I_VOL",	"H5I_GENPROP_CLS", "H5I_GENPROP_LST",
				    "H5I_ERROR_CLASS", "H5I_ERROR_MSG", "H5I_ERROR_STACK", "H5I_SPACE_SEL_ITER",
				    "H5I_EVENTSET" };
	return types[type];
}

static const char *parh5A_get_object_name(const H5VL_loc_params_t *loc_params)
{
	if (NULL == loc_params)
		return NULL;

	if (loc_params->type != H5VL_OBJECT_BY_NAME)
		return NULL;

	return loc_params->loc_data.loc_by_name.name;
}

static const char *parh5A_get_datatype_name(hid_t type_id)
{
	H5T_class_t class_id = H5Tget_class(type_id);

	switch (class_id) {
	case H5T_INTEGER:
		return "H5T_INTEGER";
	case H5T_FLOAT:
		return "H5T_FLOAT";
	case H5T_STRING:
		return "H5T_STRING";
	case H5T_COMPOUND:
		return "H5T_COMPOUND";
	// Add more cases for other datatypes as needed
	default:
		return "Unknown datatype";
	}
}

static void parh5A_print_dataspace_info(hid_t space_id)
{
	int ndims = H5Sget_simple_extent_ndims(space_id);
	log_debug("Number of dimensions: %d\n", ndims);

	hsize_t dims[ndims];
	H5Sget_simple_extent_dims(space_id, dims, NULL);

	log_debug("<Dimensions>");
	for (int i = 0; i < ndims; i++) {
		printf("%llu ", (unsigned long long)dims[i]);
	}
	log_debug("</Dimensions>");
}

void *parh5A_create(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name, hid_t type_id,
		    hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req)
{
	(void)loc_params;
	(void)attr_name;
	(void)type_id;
	(void)space_id;
	(void)acpl_id;
	(void)aapl_id;
	(void)dxpl_id;
	(void)req;

	if (loc_params->obj_type != H5I_DATASET && loc_params->obj_type != H5I_GROUP) {
		log_fatal("Sorry handling attributes only for datasets at the moment");
		_exit(EXIT_FAILURE);
	}

	parh5A_attribute_t attr = calloc(1UL, sizeof(struct parh5A_attribute));
	attr->parent.type = loc_params->obj_type;
	parh5I_inode_t inode = NULL;
	par_handle parallax_db = NULL;
	if (H5I_DATASET == attr->parent.type) {
		attr->parent.dataset = obj;
		inode = parh5D_get_inode(obj);
		parallax_db = parh5F_get_parallax_db(parh5D_get_file(obj));
	}

	if (H5I_GROUP == attr->parent.type) {
		attr->parent.group = obj;
		inode = parh5G_get_inode(obj);
		parallax_db = parh5G_get_parallax_db(obj);
	}

	parh5D_dataset_t dataset = (parh5D_dataset_t)obj;
	log_debug("Creating attribute for dataset %s location by name requested: %s attr_name: %s type: %s",
		  parh5D_get_dataset_name(dataset), parh5A_get_object_name(loc_params), attr_name,
		  parh5A_get_datatype_name(type_id));

	attr->type = H5I_ATTR;
	attr->type_id = H5Tcopy(type_id);
	attr->space_id = H5Scopy(space_id);
	log_debug("**********------>Attributes data space info:");
	parh5A_print_dataspace_info(attr->space_id);

	if (strlen(attr_name) + 1 > PARH5A_MAX_ATTR_NAME) {
		log_fatal("Attr name too large max is %u got: %lu", PARH5A_MAX_ATTR_NAME, strlen(attr_name) + 1);
		_exit(EXIT_FAILURE);
	}
	strcpy(attr->name, attr_name);
	parh5I_increase_nlinks(inode);
	parh5A_store_attr(attr);
	parh5I_store_inode(inode, parallax_db);
	return attr;
}

void *parh5A_open(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name, hid_t aapl_id, hid_t dxpl_id,
		  void **req)
{
	(void)obj;
	(void)loc_params;
	(void)attr_name;
	(void)aapl_id;
	(void)dxpl_id;
	(void)req;
	H5I_type_t *type = obj;
	if (H5I_FILE == *type)
		log_debug("Opening attribute for FILE");
	if (H5I_GROUP == *type)
		log_debug("Opening attribute for GROUP");
	if (H5I_DATASET == *type) {
		log_debug("Opening attribute: %s for DATASET: %s", attr_name, parh5D_get_dataset_name(obj));
	}
	struct parh5A_object parent = { .type = *type, .group = obj };
	parh5A_attribute_t attr = parh5A_get_attr(attr_name, &parent);
	log_debug("Open attr length of value is: %lu", strlen(attr->value) + 1);
	return attr;

	// log_debug("Sorry unimplemented method");
	// _exit(EXIT_FAILURE);
}

#define PARH5A_MAX_ATTR_SIZE 128
herr_t parh5A_read(void *_attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req)
{
	(void)buf;
	(void)dxpl_id;
	(void)req;
	H5I_type_t *type = _attr;
	if (H5I_ATTR != *type) {
		log_fatal("Corruption object is not a Parallax attribute");
		_exit(EXIT_FAILURE);
	}

	parh5A_attribute_t attr = _attr;
	size_t dst_size = H5Tget_size(mem_type_id);
	size_t src_size = H5Tget_size(attr->type_id);

	if (src_size > dst_size) {
		log_fatal("Buffer too small cannot fit the attribute");
		_exit(EXIT_FAILURE);
	}

	char temp_buf[PARH5A_MAX_ATTR_SIZE] = { 0 };
	char *intermediate_buf = attr->value;
	if (!H5Tequal(attr->type_id, mem_type_id)) {
		log_debug("Ooops type conversion needed");
		if (src_size > PARH5A_MAX_ATTR_SIZE) {
			log_fatal("Temp buf too small for conversion");
			_exit(EXIT_FAILURE);
		}
		intermediate_buf = temp_buf;
		if (H5Tconvert(attr->type_id, mem_type_id, 1, attr->value, intermediate_buf, H5P_DEFAULT) < 0) {
			log_fatal("Failed to convert types");
			_exit(EXIT_FAILURE);
		}
	}

	memcpy(buf, intermediate_buf, attr->value_size);
	log_debug("Reading attr: %s value: size: %u buf size: %lu contents:", attr->name, attr->value_size,
		  strlen(buf));

	parh5A_print_buffer_Hex((const unsigned char *)intermediate_buf, attr->value_size);

	return PARH5_SUCCESS;
}

herr_t parh5A_write(void *obj, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req)
{
	(void)dxpl_id;
	(void)req;
	H5I_type_t *type = obj;
	if (*type != H5I_ATTR) {
		log_fatal("Sorry obj should be an attribute");
		_exit(EXIT_FAILURE);
	}
	parh5A_attribute_t attr = obj;
	if (!H5Tequal(mem_type_id, attr->type_id)) {
		log_fatal("Sorry types mismatch");
		_exit(EXIT_FAILURE);
	}
	log_debug("Before-->Attr value size: %u actual mem type size: %lu buf actual size: %lu", attr->value_size,
		  H5Tget_size(mem_type_id), strlen(buf));
	if (attr->value_size < H5Tget_size(mem_type_id)) {
		attr->value = calloc(1UL, H5Tget_size(mem_type_id));
		attr->value_size = H5Tget_size(mem_type_id);
	}
	log_debug("After-->Attr value size: %u actual mem type size: %lu", attr->value_size, H5Tget_size(mem_type_id));
	memcpy(attr->value, buf, H5Tget_size(mem_type_id));
	log_debug("Contents of attribute before storing in Parallax are");
	parh5A_print_buffer_Hex((const unsigned char *)buf, attr->value_size);
	raise(SIGINT);
	parh5A_store_attr(attr);
	return PARH5_SUCCESS;
}

herr_t parh5A_get(void *obj, H5VL_attr_get_args_t *args, hid_t dxpl_id, void **req)
{
	(void)dxpl_id;
	(void)req;
	H5I_type_t *type = obj;
	if (*type != H5I_ATTR) {
		log_fatal("Sorry obj should be an attribute");
		_exit(EXIT_FAILURE);
	}
	parh5A_attribute_t attr = obj;

	if (H5VL_ATTR_GET_SPACE == args->op_type) {
		args->args.get_space.space_id = H5Scopy(attr->space_id);
		log_debug("OK answered H5VL_ATTR_GET_SPACE for attr: %s space_id: %ld", attr->name, attr->space_id);
		return PARH5_SUCCESS;
	}

	if (H5VL_ATTR_GET_TYPE == args->op_type) {
		args->args.get_type.type_id = H5Tcopy(attr->type_id);
		log_debug("OK answered H5VL_ATTR_GET_TYPE for attr: %s", attr->name);
		return PARH5_SUCCESS;
	}
	log_debug("Sorry unimplemented method for type: %d XXX TODO XXX", args->op_type);
	_exit(EXIT_FAILURE);
}

herr_t parh5A_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_args_t *attr_query,
		       hid_t dxpl_id, void **req)
{
	if (loc_params->obj_type != H5I_DATASET) {
		log_fatal("Sorry handling attributes only for datasets at the moment");
		_exit(EXIT_FAILURE);
	}

	struct parh5A_object pobject = { .type = loc_params->obj_type };
	if (H5I_DATASET == pobject.type)
		pobject.dataset = obj;

	if (H5I_GROUP == pobject.type)
		pobject.group = obj;

	// log_debug("Dataset %s location by name requested attribute: %s", parh5D_get_dataset_name(dataset),
	// 	  parh5A_get_object_name(loc_params));

	if (attr_query->op_type == H5VL_ATTR_DELETE) {
		log_debug("H5VL_ATTR_DELETE requested: %s", attr_query->args.del.name);
		if (parh5A_delete_attr(&pobject, attr_query->args.del.name))
			return PARH5_SUCCESS;
	} else if (attr_query->op_type == H5VL_ATTR_DELETE_BY_IDX) {
		log_debug("H5VL_ATTR_DELETE_BY_IDX requested");
	} else if (attr_query->op_type == H5VL_ATTR_EXISTS) {
		bool exists = parh5A_exists_attr(attr_query->args.exists.name, &pobject);
		log_debug("Does attr: %s exists?: %s", attr_query->args.exists.name, exists ? "YES" : "NO");
		*attr_query->args.exists.exists = exists;
		return PARH5_SUCCESS;
	} else if (attr_query->op_type == H5VL_ATTR_ITER) {
		log_debug("H5VL_ATTR_ITER requested");
	} else if (attr_query->op_type == H5VL_ATTR_RENAME) {
		log_debug("H5VL_ATTR_RENAME requested");
	} else {
		log_fatal("Unkown operation?");
		_exit(EXIT_FAILURE);
	}
	(void)loc_params;
	(void)attr_query;
	(void)dxpl_id;
	(void)req;
	log_debug("Requested %s", parh5A_HPItype_to_string(loc_params->obj_type));
	log_debug("Sorry unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5A_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
	(void)obj;
	(void)args;
	(void)dxpl_id;
	(void)req;
	log_debug("Sorry unimplemented method");
	_exit(EXIT_FAILURE);
}

herr_t parh5A_close(void *obj, hid_t dxpl_id, void **req)
{
	(void)dxpl_id;
	(void)req;
	H5I_type_t *type = obj;
	if (*type != H5I_ATTR) {
		log_debug("Not an attribute object");
		_exit(EXIT_FAILURE);
	}
	parh5A_attribute_t attr = obj;
	if (H5Tclose(attr->type_id) < 0) {
		log_fatal("Failed to close type for attribute: %s", attr->name);
		_exit(EXIT_FAILURE);
	}
	if (H5Sclose(attr->space_id) < 0) {
		log_fatal("Failed to close space for attribute: %s", attr->name);
		_exit(EXIT_FAILURE);
	}
	free(attr->value);
	log_debug("Closed attr %s", attr->name);
	free(obj);

	return PARH5_SUCCESS;
}
