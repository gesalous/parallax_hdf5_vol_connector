#ifndef PARALLAX_VOL_DATASET_H
#define PARALLAX_VOL_DATASET_H
#include <H5VLconnector.h>
typedef struct parh5D_dataset *parh5D_dataset_t;
typedef struct parh5I_inode *parh5I_inode_t;
typedef struct parh5F_file *parh5F_file_t;

/*VOL-plugin specific functions*/
void *parh5D_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id,
		    hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
void *parh5D_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id,
		  void **req);
herr_t parh5D_read(size_t count, void *dset[], hid_t mem_type_id[], hid_t mem_space_id[], hid_t file_space_id[],
		   hid_t dxpl_id, void *buf[], void **req);
herr_t parh5D_write(size_t count, void *dset[], hid_t mem_type_id[], hid_t mem_space_id[], hid_t file_space_id[],
		    hid_t dxpl_id, const void *buf[], void **req);
herr_t parh5D_get(void *obj, H5VL_dataset_get_args_t *args, hid_t dxpl_id, void **req);
herr_t parh5D_specific(void *obj, H5VL_dataset_specific_args_t *args, hid_t dxpl_id, void **req);
herr_t parh5D_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
herr_t parh5D_close(void *dset, hid_t dxpl_id, void **req);

/*Non VOL specific functions*/

parh5D_dataset_t parh5D_open_dataset(parh5I_inode_t inode, parh5F_file_t file);
const char *parh5D_get_dataset_name(parh5D_dataset_t dataset);
parh5I_inode_t parh5D_get_inode(parh5D_dataset_t dataset);
parh5F_file_t parh5D_get_file(parh5D_dataset_t dataset);

uint32_t parh5D_get_tile_size_in_elems(parh5D_dataset_t dataset);
uint32_t parh5D_get_elems_size_in_bytes(parh5D_dataset_t dataset);
#endif
